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
Oh, hum, this is a kind of very simple meta-ORM. It allows SeSQL to
work with different ORMs...

This is an abstract class that must be subclassed for each ORM

Even if it is not part of the OrmAdapter class, the OrmAdapter is
responsible of cathing signals and hooking itself to call the
index/delete functions as required
"""

class OrmAdapter(object):
    """
    Abstract class for SeSQL ORM adapaters
    """

    not_found = Exception # Give a more specific exception for Not Found
    node_class = None # Give a Q-compatible tree implementation

    #
    # Cursor/transaction API
    #

    def cursor(self):
        """
        Give a database cursor for raw queries
        """
        raise NotImplementedError

    def begin(self):
        """
        Get a cursor with an open transaction
        """
        cursor = self.cursor()
        cursor.execute('BEGIN')
        return cursor

    def commit(self, cursor):
        """
        Commit transaction on cursor
        """
        cursor.execute('COMMIT')

    def rollback(self, cursor):
        """
        Rollback transaction on cursor
        """
        cursor.execute('ROLLBACK')

    def transactional(self, function):
        """
        Wrap a function to have a transaction-bound cursor
        """
        def inner(*args, **kwargs):
            cursor = self.begin()
            try:
                res = function(cursor, *args, **kwargs)
                self.commit(cursor)
                return res
            except:
                self.rollback(cursor)
                raise
        inner.__name__ = function.__name__
        return inner


    #
    # More specific methods
    #

    def table_exists(self, cursor, table):
        """
        Check if the table exists
        """
        cursor.execute("SELECT count(*) FROM pg_catalog.pg_tables WHERE tablename=%s", (table,))
        return cursor.fetchone()[0]

    def load_object(self, klass, oid):
        """
        Load an object from its class and id
        """
        raise NotImplementedError
        
    def historize(self, **kwargs):
        """
        Historize data to SearchHit
        """
        raise NotImplementedError
    
