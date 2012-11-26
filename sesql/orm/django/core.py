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

    def commit(self, cursor):
        """
        Commit sub-transaction on cursor
        """
        super(DjangoOrmAdapter, self).commit(cursor)    
        transaction.commit_unless_managed()
