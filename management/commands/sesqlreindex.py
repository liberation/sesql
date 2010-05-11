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

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection, transaction
import settings
from sesql.results import SeSQLResultSet
from sesql.index import index
from sesql.typemap import typemap
import sesql_config as config
import sys, time

STEP = 1000

class Command(BaseCommand):
    help = "Reindex missing objects into SeSQL"

    @transaction.commit_manually
    def handle(self, *apps, **options):
        """
        Handle the command
        """
        if len(apps) != 1:
            print "Syntax : sesqlreindex <classname>"
            sys.exit(1)

        classname = apps[0]
        klass = typemap.get_class_by_name(classname)
        allids = set([ int(a['id']) for a in klass.objects.values('id') ])

        cursor = connection.cursor()
        cursor.execute("SELECT id FROM %s WHERE classname=%%s" % config.MASTER_TABLE_NAME,
                       (classname,))
        already = set([ int(c[0]) for c in cursor ])

        missing = allids - already

        print "%d object(s), %d already indexed, reindexing %d" % (len(allids),
                                                                   len(already),
                                                                   len(missing))

        now = start = time.time()
        nb = len(missing)
        
        for i, oid in enumerate(missing):
            obj = SeSQLResultSet.load((classname, oid))
            index(obj)

            if i % STEP == STEP - 1:
                transaction.commit()               
                last = now
                now = time.time()
                elapsed = now - start
                elapsed_last = now - last
                done = float(i) / float(nb)
                eta = elapsed / done * (1 - done)
                print "Reindexed %d objects in %.2f seconds, rate %.2f" % (STEP,
                                                                           elapsed_last,
                                                                           STEP / elapsed_last)
                print "In total, %d / %d ( %04.1f %% ) in %.2f s, rate %.2f, ETA %.2f s" % (i + 1, nb, 100 * done, elapsed, i / elapsed, eta)
        
        
        
