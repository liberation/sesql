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
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q

import settings
from sesql.results import SeSQLResultSet
from sesql.index import update
from sesql.typemap import typemap
from sesql.longquery import longquery
from sesql import utils
import sesql_config as config

import sys, time
from optparse import make_option

STEP = 1000

class Command(BaseCommand):
    help = "Update some columns of all already indexed objects in SeSQL"

    option_list = BaseCommand.option_list + (
        make_option('--class',
                    dest='class',
                    default='',
                    help='Limit to given classes (comma-separated)'),
        )

    @transaction.commit_manually
    def update(self, classnames, fields):
        """
        Reindex a single class
        """
        print "=> Starting reindexing columns %s." % ','.join(fields)
        result = longquery(Q(classname__in = classnames))
        nb = len(result)
        print "=> We got %d objects." % nb
        sys.stdout.flush()

        full_tmr = utils.Timer()
        load_tmr = utils.Timer()
        index_tmr = utils.Timer()
        broken = 0
        

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
            print "**SeSQL update step stats**"
            print " - %d objects in %.2f s, rate %.2f" % (STEP, elapsed_last,STEP / elapsed_last)
            lt = load_tmr.peek()
            it = index_tmr.peek()
            tt = (lt + it) / 100.0
            print " - loading: %.2f s (%04.1f %%), indexing: %.2f s (%04.1f %%)" % (lt, lt / tt, it, it / tt)
            print "**SeSQL global update stats**"
            print " - %d / %d ( %04.1f %% ) in %.2f s, rate %.2f, ETA %.2f s" % (i + 1, nb, 100 * done, elapsed, i / elapsed, eta)
            lt = load_tmr.get_global()
            it = index_tmr.get_global()
            tt = (lt + it) / 100.0
            print " - loading: %.2f s (%04.1f %%), indexing: %.2f s (%04.1f %%)" % (lt, lt / tt, it, it / tt)
            sys.stdout.flush()
            full_tmr.start()


        for i, obj in enumerate(result.objs):
            with load_tmr:
                try:
                    obj = result.load(obj)
                except ObjectDoesNotExist:
                    obj = None
                    broken += 1
                    log.warning("Object %r does not exist ! Broken index ?" % (obj,))
                except:
                    transaction.rollback()
                    raise
            with index_tmr:
                try:
                    update(obj, fields)
                except:
                    transaction.rollback()
                    raise                    

            if i % STEP == STEP - 1:
                disp_stats()

            del obj

        disp_stats()
    
    def handle(self, *fields, **options):
        """
        Handle the command
        """
        if not fields:
            print "Syntax : manage.py sesqlupdate [--class <classes>] <columns>"
            print "  - classes is a comma-separated list of object classes"
            print "  - columns is a (space-seperated) list of columns to reindex"
        
        if not options['class']:
            classes = typemap.all_class_names()
        else:
            classes = options['class'].split(',')

        self.update(classes, fields)            

        
        
