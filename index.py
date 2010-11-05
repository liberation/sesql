# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# SeSQL is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with SeSQL.  If not, see <http://www.gnu.org/licenses/>.

import sesql_config as config
from sesql.typemap import typemap
from sesql.fieldmap import fieldmap
from sesql import utils
from django.db import connection

import logging
log = logging.getLogger('sesql')

def load_values(obj):
    """
    Load all values of the object
    """
    values = {}
    for field in config.FIELDS:
        values[field.name] = field.get_value(obj)
    return values

@utils.log_time
def index(obj, noindex = False, values = None):
    """
    Index a Django object into SeSQL
    """
    cursor = connection.cursor()    

    # Handle dependancies
    gro = getattr(obj, "get_related_objects_for_indexation", None)
    if gro:
        related = gro()
        nbrelated = len(related)
        for item in related:
            if hasattr(item, "id"):
                # Django object ? fecth class and id
                item = (item.__class__.__name__, item.id)
            cursor.execute("SELECT nextval('sesql_reindex_id_seq')")
            cursor.execute("INSERT INTO sesql_reindex_schedule (rowid, classname, objid) SELECT currval('sesql_reindex_id_seq'), %s, %s", item)
    else:
        nbr = 0        

    table_name = typemap.get_table_for(obj.__class__)
    if not table_name:
        return

    if not values:
        values = load_values(obj)
    
    query = "DELETE FROM %s WHERE id=%%s AND classname=%%s" % table_name
    cursor.execute(query, (values["id"], values["classname"]))

    entry = "%s:%s" % (values["classname"], values["id"])

    if noindex:
        return
    
    if config.SKIP_CONDITION and config.SKIP_CONDITION(values):
        log.info("Not indexing entry %s from table %s because of skip_condition" % (entry, table_name))
        return
    
    log.info("Indexing entry %s in table %s (%d dependancies)" % (entry, table_name, nbrelated))
    
    keys = [ ]
    results = [ ]
    placeholders = [ ]

    for field in config.FIELDS:
        value = values.get(field.name, None)
        if value:
            keys.append(field.data_column)
            results.append(value)
            placeholders.append(field.placeholder)

    query = "INSERT INTO %s (%s) VALUES (%s)" % (table_name,
                                                 ",".join(keys),
                                                 ",".join(placeholders))
    cursor.execute(query, results)
    cursor.close()

def unindex(obj):
    """
    Unindex the object
    """
    return index(obj, noindex = True)

def update(obj, fields, values = None):
    """
    Update only specific fields of given object
    """
    table_name = typemap.get_table_for(obj.__class__)
    if not table_name:
        return

    if not values:
        values = load_values(obj)
    
    pattern = [ ]
    results = [ ]
    for field in fields:
        field = fieldmap.get_field(field)
        value = values.get(field.name, None)
        if value:
            pattern.append('%s=%s' % (field.data_column, field.placeholder))
            results.append(value)

    if not pattern:
        return

    pattern = ",".join(pattern)

    query = "UPDATE %s SET %s WHERE classname=%%s AND id=%%s" % (table_name,
                                                                 pattern)
    cursor = connection.cursor()    
    cursor.execute(query, results + [ values['classname'], values['id'] ])
    cursor.close()

    
