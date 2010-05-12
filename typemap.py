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

"""
Handle the type map
"""

from django.db import models
import sesql_config as config

class TypeMap(object):
    """
    Handle the classes <=> table mapping
    """
    def __init__(self):
        """
        Constructor
        """
        self.tables = {}
        self.classes = {}
        self.class_names = {}

        for klass, table in config.TYPE_MAP:
            # Ensure table exists in the map
            self.tables[table] = []

            # Now, for each subclasses...
            subclasses = self.all_subclasses(klass)
            for sc in subclasses:
                if not sc in self.classes:
                    self.classes[sc] = table
                    self.class_names[sc.__name__] = sc

        # And now fill the reverse lookup, we can only do it now, because the
        # same class can be reachable twice
        for klass, table in self.classes.items():
            self.tables[table].append(klass)

    @staticmethod
    def all_subclasses(klass, done = None):
        """
        Get all subclasses of a given class
        """
        if done is None:
            done = set()

        if klass in done:
            return []

        res = [ klass ]
        done.add(klass)
        for sc in klass.__subclasses__():
            res += TypeMap.all_subclasses(sc, done)
            
        return res
        
    def all_tables(self):
        """
        List all tables
        """
        return self.tables.keys()

    def all_classes(self):
        """
        List all classes
        """
        return self.classes.keys()

    def all_class_names(self):
        """
        List all class names
        """
        return self.class_names.keys()

    def get_class_names_for(self, table):
        """
        Get the name of classes for this table
        """
        return [ k.__name__ for k in self.get_classes_for(table) ]

    def get_classes_for(self, table):
        """
        Get the list of classes for this table
        """
        return self.tables.get(table, [])

    def get_table_for(self, klass):
        """
        Get the table for this klass
        """
        return self.classes[self.get_class_by_name(klass)]

    def get_class_by_name(self, klass):
        """
        Get the real Django class from its name
        """
        if isinstance(klass, (str, unicode)):
            return self.class_names[klass]
        return klass

typemap = TypeMap()
