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

from django.core.management.base import BaseCommand
from django.db import transaction
import settings
from sesql import utils, index, typemap, fieldmap, fields, datamodel, results
import sesql_config as config
import sys, time, cPickle, imp, os, datetime
from collections import defaultdict
from optparse import make_option

def make_zero():
    """Always return 0; we can't use a lambda since we can't pickle them"""
    return 0

class Command(BaseCommand):
    help = "Asynchronously reindex objects into another set of tables"

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
        make_option('--estimate-every',
                    dest='estimate-every',
                    type='int',
                    default=10,
                    help='Rounds between two more accurate, but slower, ETA estimate (default: 10)'),
        make_option('--since',
                    dest='since',
                    type='float',
                    default=None,
                    help='Only items that changed since given number of days (float)'),
        make_option('--state',
                    dest='state',
                    default=None,
                    help='Use given file as state file (allow graceful restart where it stopped)'),
        make_option('--config',
                    dest='config',
                    default=None,
                    help='Use given file as the new sesql_config'),
        make_option('--datefield',
                    dest='datefield',
                    default=None,
                    help='Name of the field that holds the indexation/modification date'),
        make_option('--verbose',
                    dest='verbose',
                    default=False,
                    action="store_true",
                    help='Be very verbose'),        
        make_option('-r', '--reverse',
                    dest='reverse',
                    default=False,
                    action="store_true",
                    help='Reverse mode : update current config from the other one'),
        make_option('-f', '--forever',
                    dest='forever',
                    default=False,
                    action="store_true",
                    help='Continue to watch and reindex new items forever'),
        )

    @transaction.commit_on_success
    def iteration(self):
        """
        Perform one iteration : reindex everything
        """
        self.switch_to_old()
        cursor = config.orm.cursor()
        
        opts = (self.options['datefield'], config.MASTER_TABLE_NAME)
        query = 'SELECT classname, id, %s FROM %s WHERE true' % opts
        
        vals = ()
        if self.state['since']:
            query += ' AND %s >= %%s' % (self.options['datefield'],)
            vals += (self.state['since'],)
        if self.state['last']:
            query += ' AND (classname != %s OR id != %s)'
            vals += self.state['last']
            
        query += ' ORDER BY %s ASC LIMIT %d' % (self.options['datefield'],
                                                self.options['step'])
        if self.options['verbose']:
            print "Performing %s (%s)" % (query, vals)
        cursor.execute(query, vals)
        self.switch_to_new()
        nb = 0
        for item in cursor:
            if self.options['verbose']:
                print "Indexing %s" % (item,)
            obj = results.SeSQLResultSet.load((item[0], item[1]))
            index.index(obj, index_related = False)
            last = item[2]
            nb += 1
        if nb:
            self.state['last'] = (item[0], item[1])
            self.state['since'] = last
            self.state['nb'] = nb
            self.state['done'] = self.state.get('done', 0) + nb
        return nb

    def switch_to_new(self):
        """
        Switch to the new configuration file
        """
        self.switch_to_config(self.options['config'])

    def switch_to_old(self):
        """
        Switch to the old configuration file
        """
        self.switch_to_config(self.options['config_old'])

    def switch_to_config(self, filename):
        """
        Switch to this configuration file
        """
        filename = os.path.abspath(filename)
        filename = filename.rstrip('c')
        imp.load_module('sesql_config', open(filename), filename, ('.py', 'r', imp.PY_SOURCE))
        reload(typemap)
        reload(fieldmap)
        reload(datamodel)
        reload(index)

    def count(self, since = None):
        """
        Count the remaining
        """
        cursor = config.orm.cursor()
        query = 'SELECT COUNT(*) FROM %s' % config.MASTER_TABLE_NAME
        vals = ()
        if since:
            query += ' WHERE %s >= %%s' % (self.options['datefield'],)
            vals += (self.state['since'],)
        if self.options['verbose']:
            print "Performing %s (%s)" % (query, vals)
        cursor.execute(query, vals)
        res = cursor.fetchone()[0]
        if self.options['verbose']:
            print "Result is : %s" % res
        return res

    def display_status(self):
        """
        Display current status
        """
        self.switch_to_old()

        delta = self.state['end_time'] - self.state['start_time']
        self.state['cumulated'] += delta

        # Every estimate-every chunk, we revise our progress indicator
        cur = int(self.state['done'] / self.options['step'])
        last = int((self.state['done'] - self.state['nb']) / self.options['step'])
        cur /= self.options['estimate-every']
        last /= self.options['estimate-every']
        self.state['remaining'] -= self.state['nb']
        if cur != last:
            old = self.state['remaining']
            self.state['remaining'] = self.count(self.state['since']) 
            self.state['drift'] = self.state['remaining'] - old
            self.state['cumulated_drift'] += self.state['drift']
            
        remaining = self.state['remaining']
        drift = self.state['cumulated_drift']
        if not drift:
            total = self.state['initial']
        else:            
            drift_rate = float(drift) / self.state['done']
            if drift_rate > 1.0:
                print "!!WARNING!! Drift rate is >1.0. Operation will never finish."
                return
            # We need to compute a sum of a serie, since everytime
            # we'll index n items, we'll get drift_rate * n additional
            # items
            # So we need : sum_n=0^+oo (drift_rate^k) * remaining
            # Which is ... 1/(1-drift_rate) * remaining, as we all know
            cumulated_drift_rate = 1/(1-drift_rate)
            total = self.state['initial'] * cumulated_drift_rate
            drift_estimated = remaining * cumulated_drift_rate
            
        done = self.state['done']
        percent = total and (done * 100.0 / total) or 100.0
        cumulated = self.state['cumulated']
        if percent == 100.0 or percent == 0.0:
            eta = 0
        else:
            eta = cumulated / percent * (100.0 - percent)

        print "%.2f %% done in %.2f seconds; ETA : %.2f seconds" % (percent, cumulated, eta)
        
        if drift:
            print " -> %d done, drift rate : %.2f, estimated total: %d, actual remaining: %d, estimated remaining: %d" % (done, drift_rate, total, remaining, drift_estimated)
                                                                             

    @transaction.commit_on_success
    def create_tables(self):
        """
        Create the new tables, if needed
        """
        cursor = config.orm.cursor()
        datamodel.sync_db(cursor)        

    def handle(self, **options):
        """
        Handle the command
        """
        try:
            self.work(**options)
        except KeyboardInterrupt:
            if self.options['state']:
                print "Interruped - since you used a state file, you can restart where you were."
            else:
                print "Interruped - no state file in use, you'll have to restart manually."
            sys.exit(0)
            
    def work(self, **options):
        """
        Really handle the command
        """
        self.options = options

        # Ensure we have a okish configuration file
        if not self.options['config']:
            print "--config is mandatory"
            sys.exit(1)
        if not self.options['config'].endswith('/sesql_config.py'):
            print "Configuration file must be called sesql_config.py"
            sys.exit(1)
        if not os.path.exists(self.options['config']):
            print "Configuration file %s doesn't exist" % self.options['config']
            sys.exit(1)
    
        # Ensure we provide a valid datefield
        if not self.options["datefield"]:
            print "--datefield is mandatory"
            sys.exit(1)
        field = fieldmap.fieldmap[self.options["datefield"]]
        if not isinstance(field, fields.DateTimeField):
            print "--datefield argument must be a pre-existing DateTimeField"
            sys.exit(1)            

        # Open state file if any
        self.state = defaultdict(make_zero)
        if self.options['state']:
            if self.options['since']:
                print "--since and --state are exclusive"
                sys.exit(1)
            if os.path.exists(self.options['state']):
                self.state = cPickle.load(open(self.options['state']))
        else:
            if self.options['since']:
                now = datetime.datetime.now()
                since = now  - datetime.timedelta(self.options['since'])
                self.state['since'] = since

        if not 'initial' in self.state:
            self.state['initial'] = self.count(self.state['since']) 
            self.state['remaining'] = self.state['initial']

        # Ensure new config file is ok
        if options['reverse']:
            self.options['config_old'] = self.options['config']
            self.options['config'] = config.__file__
        else:
            self.options['config_old'] = config.__file__
        self.switch_to_old()
        tables = set([ tm[1] for tm in config.TYPE_MAP if tm[1] ])
        self.switch_to_new()
        new_tables = set([ tm[1] for tm in config.TYPE_MAP if tm[1] ])
        if tables & new_tables:
            print "New and old table names should not conflict"
            sys.exit(1)

        # Create new tables, if needed
        self.create_tables()

        # Perform iterations
        while True:
            self.state['start_time'] = time.time()
            nb = self.iteration()
            self.state['end_time'] = time.time()
            self.display_status()
            if self.options['state']:
                cPickle.dump(self.state, open(self.options['state'], 'w'), 0)
            time.sleep(self.options['delay'])
            if nb < (self.options['step'] - 1) and not self.options['forever']:
                break
            
