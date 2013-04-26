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
This is a SeSQL simple benchmarking tool.
"""
# Allow "with" with python2.5
from __future__ import with_statement

import sys
import time
import random
import threading
from optparse import make_option

from sesql.index import index
from sesql.utils  import Timer
from sesql.shortquery import shortquery
from sesql.longquery import longquery
from sesql.results import SeSQLResultSet
from sesql.typemap import typemap

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Perform benchmarks on SeSQL"

    option_list = BaseCommand.option_list + (
        make_option('-d', '--duration',
                    dest='duration',
                    type='float',
                    default=30.0,
                    help='Total test duration (default 30 s)'),
        make_option('-f', '--queryfile',
                    dest='queryfile',
                    default=None,
                    help='File containing query samples to make, one per line'),
        make_option('--short-threads',
                    dest='short-threads',
                    type='int',
                    default=1,
                    help='Number of short query threads (default 1)'),
        make_option('--short-delay',
                    dest='short-delay',
                    type='float',
                    default=0.0,
                    help='Delay between two short queries in each thread (default 0.0 s)'),
        make_option('--long-threads',
                    dest='long-threads',
                    type='int',
                    default=0,
                    help='Number of long query threads (default 0)'),
        make_option('--long-limit',
                    dest='long-limit',
                    type='int',
                    default=None,
                    help='Limit (maximum number of results) for long queries'),
        make_option('--long-order',
                    dest='long-order',
                    default=None,
                    help='Order to use for long queries'),
        make_option('--long-delay',
                    dest='long-delay',
                    type='float',
                    default=0.0,
                    help='Delay between two long queries in each thread (default 0.0 s)'),
        make_option('--index-threads',
                    dest='index-threads',
                    type='int',
                    default=0,
                    help='Number of re-indexing threads (default 0)'),
        make_option('--index-delay',
                    dest='index-delay',
                    type='float',
                    default=0.0,
                    help='Delay between two re-indexing in each thread (default 0.0 s)'),
        make_option('--index-type',
                    dest='index-type',
                    default=None,
                    help='Content-type to reindex'),
        )

    def handle(self, *apps, **options):
        """
        Handle the command
        """
        self.options = options

        # If we have at least a query thread, build list of queries
        if options["long-threads"] or options["short-threads"]:
            if not options["queryfile"]:
                print "--queryfile is mandatory if a query thread is enabled"
                sys.exit(1)
            print "Loading queries list..."
            self.queries = [ (q.strip(), eval(q)) for q in open(options["queryfile"]) if q.strip() ]

        # If we have at least a reindex thread, load list of objects
        if options["index-threads"]:
            if not options["index-type"]:
                print "--index-type is mandatory if a reindex thread is enabled"
                sys.exit(1)
            self.classname = options["index-type"]
            self.klass = typemap.get_class_by_name(self.classname)
            if not hasattr(self.klass, "objects"):
                print "No such type : ", self.classname
                sys.exit(1)
            print "Loading object ids list..."
            klass = typemap.get_class_by_name(options["index-type"])
            self.allids = [ int(a['id']) for a in klass.objects.values('id') ]

        # Load queries
        self.short = []
        self.long = []
        self.index = []
        self.threads = []
        self.lock = threading.RLock()
        self.lock.acquire()

        # Starting threads
        print "Starting threads..."
        self.start_threads(options['short-threads'], self.handle_short,
                           self.short, options["short-delay"])
        self.start_threads(options['long-threads'], self.handle_long,
                           self.long, options["long-delay"])
        self.start_threads(options['index-threads'], self.handle_index,
                           self.index, options["index-delay"])

        # Waiting
        print "Running benchmark..."
        self.running = True
        self.lock.release()
        time.sleep(options["duration"])

        # Killing threads
        print "Killing threads..."
        self.kill_threads()

        # Display results
        self.display_results()

    def start_threads(self, nb, callback, store, delay):
        """
        Start nb threads for this activity
        """
        for i in range(nb):
            thread = threading.Thread(target = self.mainloop,
                                      args = (callback, store, delay))
            self.threads.append((thread))
            thread.start()

    def mainloop(self, callback, store, delay):
        """
        Run the mainloop of each thread
        """
        self.lock.acquire()
        self.lock.release()
        timer = Timer()
        while self.running:
            with timer:
                name = callback()
            store.append((timer.peek(), name))
            time.sleep(delay)

    def kill_threads(self):
        """
        Kill all the threads
        """
        self.running = False
        for thread in self.threads:
            thread.join()

    def display_results(self):
        """
        Display the results
        """
        print ""
        print "** Results **"
        print ""
        self.display_results_for("Short queries", self.short)
        self.display_results_for("Long queries", self.long)
        self.display_results_for("Index", self.index)

    def display_results_for(self, name, values):
        """
        Display the results for one category
        """
        if values:
            print "*", name

            nb = len(values)
            total = sum([ v[0] for v in values ])
            average = total / nb

            def print_above(values, nb, threshold):
                count = len([ v for v in values if v[0] > threshold ])
                print " %d above %.2f (%.2f %%)" % (count, threshold, count * 100.0 / nb)

            print " %d values, average is %.3f, rate is %.2f" % (nb, average, nb / self.options["duration"])
            print_above(values, nb, 20.0)
            print_above(values, nb, 10.0)
            print_above(values, nb, 5.0)
            print_above(values, nb, 2.0)
            print_above(values, nb, 1.0)
            values.sort()
            values.reverse()
            print " top ten: "
            for val, name in values[:10]:
                print "  - %.2f : %s" % (val, name)

    def handle_index(self):
        """
        Handle a reindexation
        """
        objid = random.choice(self.allids)
        obj = SeSQLResultSet.load((self.classname, objid))
        index(obj)
        return "(%s, %s)" % (self.classname, objid)

    def handle_short(self):
        """
        Handle a short query
        """
        query = random.choice(self.queries)
        res = shortquery(query[1])
        return query[0] + " : %d results" % len(res)

    def handle_long(self):
        """
        Handle a long query
        """
        query = random.choice(self.queries)
        res = longquery(query[1], limit = self.options["long-limit"],
                        order = self.options["long-order"])
        return query[0] + " : %d results" % len(res)

