# -*- coding: utf-8 -*-
# Copyright (c) Pilot Systems and Libération, 2010

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with SeSQL.  If not, see <http://www.gnu.org/licenses/>.

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection, transaction
import settings
from sesql import config, typemap

class Command(BaseCommand):
    help = "Dump the commands to create SeSQL tables"
    
    def handle(self, *apps, **options):
        """
        Handle the command
        """       
        print "BEGIN;"

        print """
--
-- Remember to create ascii_french.stop with :
--  LC_ALL=fr_FR.UTF-8 iconv -f utf-8 -t ascii//TRANSLIT /usr/share/postgresql/8.4/tsearch_data/french.stop > /usr/share/postgresql/8.4/tsearch_data/ascii_french.stop
--

DROP TEXT SEARCH CONFIGURATION IF EXISTS public.simple_french;
CREATE TEXT SEARCH CONFIGURATION public.simple_french (COPY = pg_catalog.simple);

DROP TEXT SEARCH DICTIONARY IF EXISTS public.simple_french_dict;
CREATE TEXT SEARCH DICTIONARY public.simple_french_dict (
    TEMPLATE = pg_catalog.simple,
    STOPWORDS = ascii_french
);

ALTER TEXT SEARCH CONFIGURATION simple_french
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart WITH simple_french_dict;
"""
        
        print """DROP TABLE IF EXISTS %s CASCADE;
CREATE TABLE %s (
""" % (config.MASTER_TABLE_NAME, config.MASTER_TABLE_NAME)

        for field in config.FIELDS:
            print "  " + field.schema()

        print """
  PRIMARY KEY (classname, id)
);
"""

        for table in typemap.all_tables():
            condition = typemap.get_class_names_for(table)
            condition = ' OR '.join([ "classname = '%s'" % cls for cls in condition ])
            print """
CREATE TABLE %s (CHECK (%s), PRIMARY KEY (classname, id)) INHERITS (%s) ;
""" % (table, condition, config.MASTER_TABLE_NAME) 
            for field in config.FIELDS:
                print field.index(table)

            for cross in config.CROSS_INDEXES:
                print """CREATE INDEX %s_%s_index ON %s (%s);
""" % (table, "_".join(cross), table, ",".join(cross))
                
        # Add the reindex schedule table
        print """
DROP SEQUENCE IF EXISTS sesql_reindex_id_seq;
CREATE SEQUENCE sesql_reindex_id_seq;

DROP TABLE IF EXISTS sesql_reindex_schedule;
CREATE TABLE sesql_reindex_schedule (
  rowid integer NOT NULL,
  classname character varying(250) NOT NULL,
  objid integer NOT NULL,
  scheduled_at timestamp NOT NULL DEFAULT NOW(),
  reindexed_at timestamp,
  PRIMARY KEY (rowid)
  );
CREATE INDEX sesql_reindex_schedule_new_index ON sesql_reindex_schedule (reindexed_at);
CREATE INDEX sesql_reindex_schedule_update_index ON sesql_reindex_schedule (classname, rowid, reindexed_at);
"""

        print "COMMIT;"
