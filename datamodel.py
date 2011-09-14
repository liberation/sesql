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
from sesql.typemap import typemap

def sql_function(func):
    """
    Decorator to execute or print SQL statements
    """
    def sql_function_inner(cursor, execute = False, verbosity = True,
                           include_drop = False,
                           **kwargs):
        sql = func(**kwargs)
        if not include_drop:
            sql = [ row for row in sql if not row.startswith('DROP ') ]
        if verbosity:
            print
            for row in sql:
                print row + ";"
            print
        if execute:
            for row in sql:
                cursor.execute(row)
    return sql_function_inner

@sql_function
def create_dictionnary():
    """
    Create the dictionnary configuration
    """
    return [
        "DROP TEXT SEARCH CONFIGURATION IF EXISTS public.%s" % config.TS_CONFIG_NAME,
        "CREATE TEXT SEARCH CONFIGURATION public.%s (COPY = pg_catalog.simple)" % config.TS_CONFIG_NAME,
        "DROP TEXT SEARCH DICTIONARY IF EXISTS public.%s_dict" % config.TS_CONFIG_NAME,
        """CREATE TEXT SEARCH DICTIONARY public.%s_dict (
        TEMPLATE = pg_catalog.simple,
        STOPWORDS = %s
)""" % (config.TS_CONFIG_NAME, config.STOPWORDS_FILE),
        """ALTER TEXT SEARCH CONFIGURATION %s
        ALTER MAPPING FOR asciiword, asciihword, hword_asciipart WITH %s_dict""" % (config.TS_CONFIG_NAME, config.TS_CONFIG_NAME)
        ] + getattr(config, "ADDITIONAL_TS_CONFIG", [])

@sql_function
def create_master_table():
    """
    Create the master table, that is, the one from which the others
    will inherit
    """
    schema = "\n  ".join([ field.schema() for field in config.FIELDS ])
    
    return [
        "DROP TABLE IF EXISTS %s CASCADE" % config.MASTER_TABLE_NAME,
        """CREATE TABLE %s (
%s
  PRIMARY KEY (classname, id)
)""" % (config.MASTER_TABLE_NAME, schema)
        ]

@sql_function
def create_table(table = None):
    """
    Create given table
    """
    if table is None:
        return []
    
    condition = typemap.get_class_names_for(table)
    condition = ' OR '.join([ "classname = '%s'" % cls for cls in condition ])
    res = [ "CREATE TABLE %s (CHECK (%s), PRIMARY KEY (classname, id)) INHERITS (%s)" % (table, condition, config.MASTER_TABLE_NAME) ]

    for field in config.FIELDS:
        res.append(field.index(table))
        
    for cross in config.CROSS_INDEXES:
        res.append("CREATE INDEX %s_%s_index ON %s (%s);" % (table, "_".join(cross), table, ",".join(cross)))

    return res
    
@sql_function
def create_schedule_table():
    """
    Create the table to insert the reindex schedule
    """
    return [
        "DROP SEQUENCE IF EXISTS sesql_reindex_id_seq",
        "CREATE SEQUENCE sesql_reindex_id_seq",

        "DROP TABLE IF EXISTS sesql_reindex_schedule",
        """CREATE TABLE sesql_reindex_schedule (
        rowid integer NOT NULL,
        classname character varying(250) NOT NULL,
        objid integer NOT NULL,
        scheduled_at timestamp NOT NULL DEFAULT NOW(),
        PRIMARY KEY (rowid)
        )""",
        "CREATE INDEX sesql_reindex_schedule_date_index ON sesql_reindex_schedule (scheduled_at)",
        "CREATE INDEX sesql_reindex_schedule_content_index ON sesql_reindex_schedule (classname, rowid)"
    ]


@config.orm.transactional
def sync_db(cursor, verbosity = 0):
    if not config.orm.table_exists(cursor, config.MASTER_TABLE_NAME):
        create_dictionnary(cursor, execute = True, verbosity = verbosity, include_drop = True)
        create_master_table(cursor, execute = True, verbosity = verbosity, include_drop = True)
    elif verbosity:
        print "SeSQL : Table %s already existed, skipped." % config.MASTER_TABLE_NAME
        
    for table in typemap.all_tables():
        if not config.orm.table_exists(cursor, table):
            create_table(cursor, table = table, execute = True, verbosity = verbosity)
        elif verbosity:
            print "SeSQL : Table %s already existed, skipped." % table

    if not config.orm.table_exists(cursor, "sesql_reindex_schedule"):
        create_schedule_table(cursor, execute = True, verbosity = verbosity, include_drop = True)
    elif verbosity:
        print "SeSQL : Table %s already existed, skipped." % 'sesql_reindex_schedule'
