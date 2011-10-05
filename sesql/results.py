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

from typemap import typemap
import sesql_config as config

import logging
log = logging.getLogger('sesql')

class SeSQLResultSet(object):
    """
    A lazy SeSQL result set
    It mimicks a bit the Django QuerySet, but doesn't work the same way,
    and doesn't provide exactly the same methods
    """
    def __init__(self, objs):
        """
        Constructor
        Objs must be a list of (class, id)
        """
        self.objs = objs

    def count(self):
        """
        Count results
        """
        return len(self.objs)
    __len__ = count

    def iterator(self):
        """
        Iterate on self
        """
        for obj in self.objs:
            try:
                yield self.load(obj)
            except config.orm.not_found:
                log.warning("Object %r does not exist ! Broken index ?" % (obj,))
    __iter__ = iterator
    
    def all(self):
        """
        Get all the results as a list
        """
        return list(self)

    def get(self, index):
        """
        Get the row at given index
        """
        return self.load(self.objs[index])
    __getitem__ = get

    def __getslice__(self, i, j):
        """
        Get a slice
        """
        res = [ self.load(obj) for obj in self.objs[i:j] ]
        return res

    @staticmethod
    def load(obj):
        """
        Get a given object
        """
        objclass, objid = obj
        objclass = typemap.get_class_by_name(objclass)
        entry = "%s:%s" % (objclass.__name__, objid)
        log.debug("Fetching %s" % entry)
        return config.orm.load_object(objclass, objid)

    def historize(self, query):
        """save in the database the query for future processing"""
        nb_results = self.count()
        query_text = query.get_fulltext_query()[2][0]
        config.orm.historize(query=query_text, nb_results=nb_results)
        
        
