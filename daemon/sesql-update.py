#!/usr/bin/python
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
Update the scheduled updates
"""

import sys, time, traceback, os, logging

from sesql.daemon.unixdaemon import UnixDaemon
from sesql.daemon.cmdline import CmdLine

from sesql_config import *

from sesql import index, results

from django.db import connection, transaction


def version():
    print "sesql update daemon, v 0.9"

class UpdateDaemon(UnixDaemon):
    """
    The daemon class
    """
    def __init__(self, chunk, delay, pidfile):
        UnixDaemon.__init__(self, pidfile)
        self.chunk = int(chunk)
        self.delay = float(delay)
        self.log = logging.getLogger('sesql-update')

    def run(self):
        """
        Main loop
        """
        while True:
            try:
                self.process_chunk()
            except:
                type, value, tb = sys.exc_info()        
                error = traceback.format_exception_only(type, value)[0]
                print >> sys.stderr, error
            self.log.debug("Sleeping for %.2f second(s)" % self.delay)
            time.sleep(self.delay)

    @transaction.commit_manually
    def process_chunk(self):
        """
        Process a chunk
        """
        cursor = connection.cursor()    
        cursor.execute("""SELECT classname, objid
                          FROM sesql_reindex_schedule
                          ORDER BY scheduled_at ASC LIMIT %d""" % self.chunk)
        rows = cursor.fetchall()
        if not rows:
            transaction.rollback()
            return
        self.log.info("Found %d row(s) to reindex" % len(rows))

        done = set()
        
        for row in rows:
            row = tuple(row)
            if not row in done:
                self.log.info("Reindexing %s:%d" % row)
                done.add(row)
                obj = results.SeSQLResultSet.load(row)
                index.index(obj)
                cursor.execute("""DELETE FROM sesql_reindex_schedule
                                  WHERE classname=%s AND objid=%s""", row)
        transaction.commit()

if __name__ == "__main__":
    cmd = CmdLine(sys.argv)
    cmd.add_opt('debug', 'd', None, "Run in debug mode (don't daemonize)")
    cmd.add_opt('chunk', 'c', str(DAEMON_DEFAULT_CHUNK), "Chunk size")
    cmd.add_opt('wait', 'w', str(DAEMON_DEFAULT_DELAY),
                "Wait between each chunk")
    cmd.add_opt('pidfile', 'p', str(DAEMON_DEFAULT_PID), "Pidfile to use")
    cmd.parse_opt()
      
    if cmd["help"]:
        cmd.show_help()
        sys.exit(0)

    if cmd["version"]:
        version()
        sys.exit(0)

    daemon = UpdateDaemon(cmd["chunk"], cmd["wait"], cmd["pidfile"])

    if cmd["debug"]:
        ch = logging.StreamHandler()
        logger = logging.getLogger('sesql-update')
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        daemon.run()
    else:
        daemon.start_deamon()

