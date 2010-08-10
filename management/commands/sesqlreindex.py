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

# Allow "with" with python2.5
from __future__ import with_statement

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection, transaction
import settings
from sesql.results import SeSQLResultSet
from sesql.index import index
from sesql.typemap import typemap
from sesql import utils
import sesql_config as config
import sys, time
from optparse import make_option

STEP = 1000

class Command(BaseCommand):
    help = "Reindex missing objects into SeSQL"

    option_list = BaseCommand.option_list + (
        make_option('--reindex',
                    action='store_true',
                    dest='reindex',
                    default=False,
                    help='Reindex already indexed content'),
        )

    @transaction.commit_manually
    def reindex(self, classname, reindex = False):
        """
        Reindex a single class
        """
        klass = typemap.get_class_by_name(classname)
        if not hasattr(klass, "objects"):
            return

        print "=> Starting reindexing for %s" % classname
        
        allids = set([ int(a['id']) for a in klass.objects.values('id') ])

        cursor = connection.cursor()
        cursor.execute("SELECT id FROM %s WHERE classname=%%s" % config.MASTER_TABLE_NAME,
                       (classname,))
        already = set([ int(c[0]) for c in cursor ])

        if not reindex:
            missing = allids - already
        else:
            missing = allids

        print "%s : %d object(s), %d already indexed, reindexing %d" % (classname, len(allids),
                                                                   len(already),
                                                                   len(missing))

        nb = len(missing)
        full_tmr = utils.Timer()
        load_tmr = utils.Timer()
        index_tmr = utils.Timer()

        def disp_stats():
            with index_tmr:
                transaction.commit()

            if not nb:
                return

            full_tmr.stop()
            elapsed = full_tmr.get_global()
            elapsed_last = full_tmr.peek()
            done = float(i + 1) / float(nb)
            eta = elapsed / done * (1 - done)
            print "**SeSQL reindex step stats**"
            print " - %d objects in %.2f s, rate %.2f" % (STEP, elapsed_last,STEP / elapsed_last)
            lt = load_tmr.peek()
            it = index_tmr.peek()
            tt = (lt + it) / 100.0
            print " - loading: %.2f s (%04.1f %%), indexing: %.2f s (%04.1f %%)" % (lt, lt / tt, it, it / tt)
            print "**SeSQL global reindex on %s stats**" % classname
            print " - %d / %d ( %04.1f %% ) in %.2f s, rate %.2f, ETA %.2f s" % (i + 1, nb, 100 * done, elapsed, i / elapsed, eta)
            lt = load_tmr.get_global()
            it = index_tmr.get_global()
            tt = (lt + it) / 100.0
            print " - loading: %.2f s (%04.1f %%), indexing: %.2f s (%04.1f %%)" % (lt, lt / tt, it, it / tt)
            full_tmr.start()

        for i, oid in enumerate(missing):
            with load_tmr:
                obj = SeSQLResultSet.load((classname, oid))
            with index_tmr:
                index(obj)

            if i % STEP == STEP - 1:
                disp_stats()

        disp_stats()
        
    
    def handle(self, *apps, **options):
        """
        Handle the command
        """
        if not apps:
            apps = typemap.all_class_names()

        for app in apps:
            self.reindex(app, options['reindex'])            

        
        
        
