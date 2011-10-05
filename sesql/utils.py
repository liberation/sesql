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

# Allow "with" with python2.5
from __future__ import with_statement

from django.db import connection, utils
import time
import logging
log = logging.getLogger('sesql')


class Timer(object):
    """
    A timer object to be used with « with » statement
    It as a local and global timer
    """
    def __init__(self):
        self._local = self._global = 0.0
        self._start = time.time()

    def start(self):
        self._start = time.time()

    def stop(self):
        delta = time.time() - self._start
        self._local += delta
        self._global += delta

    def __enter__(self):
        self.start()

    def __exit__(self, *args, **kwargs):
        self.stop()

    def get_local(self):
        return self._local

    def get_global(self):
        return self._global

    def reset(self):
        self._local = 0

    def peek(self):
        res = self.get_local()
        self.reset()
        return res

def try_sql(sql, *args, **kwargs):
    """
    Try to run a SQL snippet, isolated in a transaction, and return
    cursor, values or None, None
    """
    cursor = connection.cursor()
    cursor.execute("COMMIT")
    cursor.execute("BEGIN")
    try:
        cursor.execute(sql, *args, **kwargs)
        # We need to fetch values before the COMMIT
        values = cursor.fetchall()
        cursor.execute("COMMIT")
        return cursor, values
    except utils.DatabaseError:
        cursor.execute("ROLLBACK")
        return None, None
    except:
        cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.execute("BEGIN")

def table_exists(table):
    """
    Check if the table exists
    """
    cursor, values = try_sql("SELECT * FROM %s LIMIT 0" % table)
    if cursor is None:
        return False

    return True

def log_time(function, message = None):
    """
    Decorator to log function call and execution time
    """
    tmr = Timer()
    def log_time_inner(*args, **kwargs):
        with tmr:
            res = function(*args, **kwargs)
        args = ', '.join([ str(a) for a in args ])
        extra = [ "%s=%s" % (key, value) for key, value in kwargs.items() ]
        kwargs = ', '.join(extra)
        m = message
        if m is None:
            m = '%s (%s, %s)' % (function.__name__, args, kwargs,)
        log.info('%s : %.2f second(s)' % (m, tmr.peek()))
        return res
    log_time_inner.__name__ = function.__name__
    return log_time_inner

                 
