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

"""
This command will massively index or reindex content into SeSQL. It is
a very useful management command, either when you first install SeSQL
on an existing database, or when you alter your indexes.
"""

# Allow "with" with python2.5
from __future__ import with_statement

import sys

from optparse import make_option

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from sesql.results import SeSQLResultSet
from sesql.index import index
from sesql.typemap import typemap
from sesql import utils
import sesql_config as config

STEP = 1000


class Command(BaseCommand):
    help = "Reindex missing objects into SeSQL. You can specify class names as additional parameters."

    option_list = BaseCommand.option_list + (
        make_option('--reindex',
                    action='store_true',
                    dest='reindex',
                    default=False,
                    help='Reindex already indexed content'),
        make_option('-o', '--order',
                    dest='order',
                    default=None,
                    help='Sort order of the content to process'),
        )

    @transaction.commit_manually
    def reindex(self, classname, reindex=False):
        """
        Reindex a single class
        """
        klass = typemap.get_class_by_name(classname)
        if not hasattr(klass, "objects"):
            return

        print "=> Starting reindexing for %s" % classname
        sys.stdout.flush()

        objs = klass.objects.values('id')
        if self.options["order"]:
            objs = objs.order_by(self.options["order"])
        allids = [int(a['id']) for a in objs]

        cursor = connection.cursor()
        cursor.execute("SELECT id FROM %s WHERE classname=%%s" % config.MASTER_TABLE_NAME,
                       (classname,))
        already = set([int(c[0]) for c in cursor])

        if not reindex:
            missing = [oid for oid in allids if not oid in already]
        else:
            missing = allids

        print "%s : %d object(s), %d already indexed, reindexing %d" % (classname, len(allids),
                                                                   len(already),
                                                                   len(missing))
        sys.stdout.flush()

        nb = len(missing)
        full_tmr = utils.Timer()

        def disp_stats():
            if not nb:
                return

            full_tmr.stop()
            elapsed = full_tmr.get_global()
            elapsed_last = full_tmr.peek()
            done = float(i + 1) / float(nb)
            eta = elapsed / done * (1 - done)
            print "**SeSQL reindex step stats**"
            print " - %d objects in %.2f s, rate %.2f" % (STEP, elapsed_last, STEP / elapsed_last)
            print "**SeSQL global reindex on %s stats**" % classname
            print " - %d / %d ( %04.1f %% ) in %.2f s, rate %.2f, ETA %.2f s" % (i + 1, nb, 100 * done, elapsed, i / elapsed, eta)
            sys.stdout.flush()
            full_tmr.start()

        for i, oid in enumerate(missing):
            obj = None
            try:
                obj = SeSQLResultSet.load((classname, oid))
                index(obj)
                transaction.commit()
            except Exception, e:
                print "Error indexing (%s,%s) : %s" % (classname, oid, e)
                transaction.rollback()
            del obj

            if i % STEP == STEP - 1:
                disp_stats()

        disp_stats()
        transaction.commit()

    def handle(self, *classes, **options):
        """
        Handle the command
        """
        self.options = options

        if not classes:
            classes = typemap.all_class_names()

        for klass in classes:
            self.reindex(klass, options['reindex'])
