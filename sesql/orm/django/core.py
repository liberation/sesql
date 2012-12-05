# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010-2012

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

from sesql.ormadapter import OrmAdapter
from django.db import connection, transaction
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q

class DjangoOrmAdapter(OrmAdapter):
    """
    ORM Adapater for Django
    """

    not_found = ObjectDoesNotExist
    node_class = Q

    def cursor(self):
        """
        Gives a database cursor for raw queries
        """
        return connection.cursor()

    def load_object(self, klass, oid):
        """
        Load an object from its class and id
        """
        return klass.objects.get(pk = oid)
        
    def historize(self, **kwargs):
        """
        Historize data to SearchHit
        """
        from models import SearchHit
        SearchHit(**kwargs).save()

    def get_txlvl(self):
        """
        Get current subtransaction level
        """
        return getattr(connection, '_sesql_subtransaction_level', 0)

    def inc_txlvl(self):
        """
        Increment the subtransaction level, return the new one
        """
        txlvl = self.get_txlvl() + 1
        connection._sesql_subtransaction_level = txlvl
        return txlvl

    def dec_txlvl(self):
        """
        Decrement the subtransaction level, return the previous one
        """
        txlvl = self.get_txlvl()
        if txlvl:
            connection._sesql_subtransaction_level = txlvl - 1
        return txlvl

    def begin(self):
        """
        Get a cursor with an open sub-transaction
        """
        cursor = self.cursor()
        txlvl = self.inc_txlvl()
        cursor.execute('SAVEPOINT sesql_savepoint_%d' % txlvl)
        return cursor

    def commit(self, cursor):
        """
        Commit sub-transaction on cursor
        """
        txlvl = self.dec_txlvl()
        if txlvl:
            cursor.execute('RELEASE SAVEPOINT sesql_savepoint_%d' % txlvl)
        if txlvl == 1:
            # Last subtransaction level ? Commit
            transaction.commit_unless_managed()            

    def rollback(self, cursor):
        """
        Rollback sub-transaction on cursor
        """
        txlvl = self.dec_txlvl()
        if txlvl:
            cursor.execute('ROLLBACK TO SAVEPOINT sesql_savepoint_%d' % txlvl)

