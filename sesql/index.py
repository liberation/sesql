# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010-2011

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
from sesql import typemap, fieldmap, utils

import logging
log = logging.getLogger('sesql')

def index_log_wrap(function):
    """
    Log wrap the method, giving it a name and logging its time
    """
    def inner(obj, *args, **kwargs):
        try:
            classname, objid = get_sesql_id(obj)
            message = "%s (%s:%s)" % (function.__name__, classname, objid)
        except (TypeError, AttributeError, ValueError):
            message = "%s (invalid object %r)" % (function.__name__, obj)
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

    for field in fields:
        keys.extend(field.index_columns)
        placeholders.extend(field.index_placeholders)
        results.extend(field.get_values(obj))

    return keys, placeholders, results

def get_sesql_id(obj):
    """
    Get classname and id, the SeSQL identifiers
    """
    if isinstance(obj, (tuple, list)) and len(obj) == 2:
        return tuple(obj)

    def get_val(field):
        vals = fieldmap.fieldmap[field].get_values(obj)
        return vals and vals[0] or None
    return (get_val('classname'), get_val('id'))

@config.orm.transactional
def schedule_reindex(cursor, item):
    try:
        item = get_sesql_id(item)
    except (TypeError, AttributeError, ValueError):
        log.info("%r: can't get classname/id, skipping" % item)
        return
        
    classname, objid = item
    table_name = typemap.typemap.get_table_for(classname)
    if not table_name:
        log.info("%s: no table found, skipping" % classname)
        return

    cursor.execute("SELECT nextval('sesql_reindex_id_seq')")
    cursor.execute("INSERT INTO sesql_reindex_schedule (rowid, classname, objid) SELECT currval('sesql_reindex_id_seq'), %s, %s", item)

@index_log_wrap
@config.orm.transactional
def index(cursor, obj, message, noindex = False, index_related = True):
    """
    Index a Django object into SeSQL, do the real work
    """
    log.info("%s : entering" % message)
    try:
        classname, objid = get_sesql_id(obj)
    except (TypeError, AttributeError, ValueError):
        log.info("%r: can't get classname/id, skipping" % obj)
        return

    # Handle dependancies
    gro = getattr(obj, "get_related_objects_for_indexation", None)
    if index_related and gro:
        related = gro()
        nbrelated = len(related)
        for item in related:
            schedule_reindex(item)
    else:
        nbrelated = 0        

    log.info("%s : %d dependancies found" % (message, nbrelated))

    table_name = typemap.typemap.get_table_for(classname)
    if not table_name:
        log.info("%s: no table found, skipping" % message)
        return

    cursor.execute('SAVEPOINT sesql_index_savepoint')

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
    cursor.execute(query, results)
        

@index_log_wrap
def unindex(obj, message):
    """
    Unindex the object
    """
    return index(obj, noindex = True)

@index_log_wrap
@config.orm.transactional
def update(cursor, obj, message, fields):
    """
    Update only specific fields of given object
    """
    log.info("%s : entering for fields %s" % (message, ','.join(fields)))

    table_name = typemap.typemap.get_table_for(obj.__class__)
    if not table_name:
        log.info("%s : not table, skipping" % message)
        return

    fields = [ fieldmap.fieldmap.get_field(field) for field in fields ]
    keys, placeholders, results = get_values(obj, fields)

    pattern = [ '%s=%s' % (k,p) for k,p in zip(keys, placeholders) ]

    if not pattern:
        log.info("%s : nothing to update, skipping" % message)
        return

    pattern = ",".join(pattern)

    query = "UPDATE %s SET %s WHERE classname=%%s AND id=%%s" % (table_name,
                                                                 pattern)
    cursor.execute(query, results + [ obj.__class__.__name__, obj.id ])

    
