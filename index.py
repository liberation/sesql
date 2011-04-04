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

def index_log_wrap(function):
    """
    Log wrap the method, giving it a name and logging its time
    """
    def inner(obj, *args, **kwargs):
        classname, objid = get_sesql_id(obj)
        message = "%s (%s:%s)" % (function.__name__, classname, objid)
        return utils.log_time(function, message)(obj, message, *args, **kwargs)
    inner.__name__ = function.__name__
    return inner

def get_values(obj, fields):
    """
    Get SQL keys, placeholders and results for this object and those fields
    """
    keys = [ ]
    placeholders = [ ]
    results = [ ]

    for field in config.FIELDS:
        keys.extend(field.index_columns)
        placeholders.extend(field.index_placeholders)
        results.extend(field.get_values(obj))

    return keys, placeholders, results

def get_sesql_id(obj):
    """
    Get classname and id, the SeSQL identifiers
    """
    return (fieldmap['classname'].get_values(obj)[0], fieldmap['id'].get_values(obj)[0])

@index_log_wrap
def index(obj, message, noindex = False):
    """
    Index a Django object into SeSQL, do the real work
    """
    cursor = connection.cursor()
    log.info("%s : entering" % message)
    classname, objid = get_sesql_id(obj)

    # Handle dependancies
    gro = getattr(obj, "get_related_objects_for_indexation", None)
    if gro:
        related = gro()
        nbrelated = len(related)
        for item in related:
            if hasattr(item, "id"):
                # Django object ? fecth class and id
                item = get_sesql_id(obj)
            cursor.execute("SELECT nextval('sesql_reindex_id_seq')")
            cursor.execute("INSERT INTO sesql_reindex_schedule (rowid, classname, objid) SELECT currval('sesql_reindex_id_seq'), %s, %s", item)
    else:
        nbrelated = 0        

    log.info("%s : %d dependancies found" % (message, nbrelated))

    table_name = typemap.get_table_for(classname)
    if not table_name:
        log.info("%s: no table found, skipping" % message)
        return

    query = "DELETE FROM %s WHERE id=%%s AND classname=%%s" % table_name
    cursor.execute(query, (objid, classname))

    if noindex:
        log.info("%s : running in 'noindex' mode, only deleteing" % message)
        return
    
    if config.SKIP_CONDITION and config.SKIP_CONDITION(obj):
        log.info("%s : not indexing because of skip_condition" % message)
        return
    
    log.info("%s : indexing entry in table %s" % (message, table_name))

    keys, placeholders, results = get_values(obj, config.FIELDS)
    
    query = "INSERT INTO %s (%s) VALUES (%s)" % (table_name,
                                                 ",".join(keys),
                                                 ",".join(placeholders))
    try:
        cursor.execute(query, results)
    except:
        log.error('Exception caught while inserting (%s,%s) into %s',
                  (classname, objid, table_name))
        raise
    cursor.close()

@index_log_wrap
def unindex(obj, message):
    """
    Unindex the object
    """
    return index(obj, noindex = True)

@index_log_wrap
def update(obj, message, fields):
    """
    Update only specific fields of given object
    """
    log.info("%s : entering for fields %s" % (message, ','.join(fields)))

    table_name = typemap.get_table_for(obj.__class__)
    if not table_name:
        log.info("%s : not table, skipping" % message)
        return

    fields = [ fieldmap.get_field(field) for field in fields ]
    keys, placeholders, results = get_values(obj, fields)

    pattern = [ '%s=%s' % (k,p) for k,p in zip(keys, placeholders) ]

    if not pattern:
        log.info("%s : nothing to update, skipping" % message)
        return

    pattern = ",".join(pattern)

    query = "UPDATE %s SET %s WHERE classname=%%s AND id=%%s" % (table_name,
                                                                 pattern)
    cursor = connection.cursor()    
    cursor.execute(query, results + [ obj.__class__.__name__, obj.id ])
    cursor.close()

    
