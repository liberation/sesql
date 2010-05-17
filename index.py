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
from django.db import connection

import logging
log = logging.getLogger('sesql')

def index(obj, noindex = False):
    """
    Index a Django object into SeSQL
    """
    if not obj.__class__ in typemap.classes:
        return
    
    cursor = connection.cursor()    

    values = {}
    for field in config.FIELDS:
        values[field.name] = field.get_value(obj)
    table_name = typemap.get_table_for(obj.__class__)    

    query = "DELETE FROM %s WHERE id=%%s AND classname=%%s" % table_name
    cursor.execute(query, (values["id"], values["classname"]))

    entry = "%s:%s" % (values["classname"], values["id"])

    if noindex:
        return
    
    if config.SKIP_CONDITION and config.SKIP_CONDITION(values):
        log.info("Not indexing entry %s from table %s because of skip_condition" % (entry, table_name))
        return
    
    log.info("Indexing entry %s in table %s" % (entry, table_name))
    
    keys = [  ]
    results = [  ]
    placeholders = [  ]

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

def unindex(obj):
    """
    Unindex the object
    """
    return index(obj, noindex = True)
