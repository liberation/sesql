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

from django.db import connection
from psycopg2 import ProgrammingError

def try_sql(sql, *args, **kwargs):
    """
    Try to run a SQL snippet, isolated in a transaction, and return
    cursor, values or None, None
    """
    cursor = connection.cursor()
    cursor.execute("BEGIN")
    try:
        cursor.execute(sql, *args, **kwargs)
        # We need to fetch values before the COMMIT
        values = cursor.fetchall()
        cursor.execute("COMMIT")
        return cursor, values
    except ProgrammingError:
        cursor.execute("ROLLBACK")
        return None, None
    except:
        cursor.execute("ROLLBACK")
        raise

def table_exists(table):
    """
    Check if the table exists
    """
    cursor, values = try_sql("SELECT * FROM %s LIMIT 0" % table)
    if cursor is None:
        return False

    return True
