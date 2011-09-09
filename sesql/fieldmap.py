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
Handle the field map
"""

from django.db import models
import sesql_config as config

class FieldMap(object):
    """
    Handle the classes <=> table mapping
    """
    def __init__(self):
        """
        Constructor
        """
        self.fields_map = {}
        self.fields = config.FIELDS
        self.primary = None

        for field in config.FIELDS:
            if field.primary:
                self.primary = field
            self.fields_map[field.name] = field

    def all_fields(self):
        """
        List all fields
        """
        return self.fields

    def get_field(self, field):
        """
        Get the real field from its name
        """
        if isinstance(field, (str, unicode)):
            return self.fields_map[field]
        return field
    __getitem__ = get_field

    def get_primary(self):
        """
        Get the primary field if any
        """
        return self.primary

fieldmap = FieldMap()
