# -*- coding: utf-8 -*-
# Copyright (c) Pilot Systems and Libération, 2012

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
This command will gradually reindex, using a new sesql_config file, to
a new set of tables, and then you can switch the tables.
"""
import sys
import time
from optparse import make_option

from django.db import transaction
from django.core.management.base import BaseCommand

from sesql import config
from sesql import typemap
from sesql.utils import print_eta


class Command(BaseCommand):
    help = "Fixate a date field by copying empty values from another"

    option_list = BaseCommand.option_list + (
        make_option('-s', '--step',
                    dest='step',
                    default=1000,
                    type='int',
                    help='Size of a step (default: 1000 items)'),
        make_option('-d', '--delay',
                    dest='delay',
                    type='float',
                    default=0.5,
                    help='Delay between two steps (default: 0.1 s) '),
        make_option('--source',
                    dest='source',
                    default=None,
                    help='Name of the source field'),
        make_option('--target',
                    dest='target',
                    default=None,
                    help='Name of the target field'),
        )

    @transaction.commit_on_success
    def iteration(self, table, idmin, idmax):
        """
        Perform one iteration : reindex everything
        """
        cursor = config.orm.cursor()
        query = '''
        UPDATE %s
        SET %s = %s
        WHERE %s IS NULL
        AND (id >= %d) AND (id <= %d)
        ''' % (table, self.target, self.source, self.target, idmin, idmax)
        cursor.execute(query)

    def process_table(self, table):
        """
        Process on given table
        """

        cursor = config.orm.cursor()
        query = 'SELECT min(id), max(id) FROM %s WHERE %s IS NULL' % (table,
                                                                      self.target)
        cursor.execute(query)
        start_time = time.time()
        idmin, idmax = cursor.fetchone()
        if idmin and idmax:
            print "Processing table %s from id %d to %d" % (table, idmin, idmax)
            start = idmin
            while start <= idmax:
                end = min(start + self.options['step'], idmax) + 1
                self.iteration(table, start, end)
                start = end
                time.sleep(self.options['delay'])
                timedelta = time.time() - start_time
                percent = float(start - idmin - 1) / float(idmax - idmin) * 100.0
                print_eta(percent, timedelta)
        else:
            print "Table %s is good, nothing to do" % table

    def handle(self, **options):
        """
        Really handle the command
        """
        self.options = options

        # Ensure we have a okish configuration file
        if not self.options['source'] or not self.options['target']:
            print "--source and --target are mandatory"
            sys.exit(1)

        self.source = self.options['source']
        self.target = self.options['target']

        for table in typemap.typemap.tables.keys():
            if table:
                self.process_table(table)


