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

from sesql.ormadapter import OrmAdapter
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker, mapper

import logging
log = logging.getLogger('sesql')

from django_q import Q

class AlchemyOrmAdapter(OrmAdapter):
    """
    Adapter to use SeSQL on SQLAlchemy
    """

    not_found = NoResultFound
    node_class = Q

    def __init__(self):
        """
        Constructor
        """
        self.engine = None

    def bind(self, connection, source_maker):
        """
        Bind to a source session maker - this is mandatory to search,
        not to index
        """
        self.source_maker = source_maker        
        self.engine = create_engine(connection, convert_unicode=True,
                                    encoding = 'utf-8', echo = True)
        self.bind_signals()

    def load_object(self, klass, oid):
        """
        Load an object from its class and id
        """
        return self.source_maker().query(klass).filter_by(id = oid).one()
        
    def historize(self, **kwargs):
        """
        Historize data to SearchHit - not implemented for now, but silently discard
        """
        return

    def bind_signals(self):
        """
        Ok ok let's bind signals !
        """
        event.listen(mapper, 'after_insert', self.update_cb)
        event.listen(mapper, 'after_update', self.update_cb)
        event.listen(mapper, 'before_delete', self.delete_cb)

    def update_cb(self, mapper, connection, target):
        """
        Object was created or deleted
        """
        from sesql import index
        return index.index(target)

    def delete_cb(self, mapper, connection, target):
        """
        Object was created or deleted
        """
        from sesql import index
        return index.unindex(target)
        
    def sync_db(self):
        """
        Create tables - must be manually called
        """
        from sesql.datamodel import sync_db
        cursor = sync_db(0)

    def cursor(self):
        """
        Get a cursor
        """
        return self.engine.raw_connection().cursor()
        
    def begin(self):
        """
        Get a cursor with an open sub-transaction
        """
        cursor = self.cursor()
        cursor.execute('BEGIN')
        return cursor

    def commit(self, cursor):
        """
        Commit sub-transaction on cursor
        """
        cursor.execute('COMMIT')

    def rollback(self, cursor):
        """
        Rollback sub-transaction on cursor
        """
        cursor.execute('ROLLBACK')

