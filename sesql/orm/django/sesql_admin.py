# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2011

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
This monkey patch will enable to use sesql in the Django admin
It's not fully optimal yet, but functionnal
"""
from django.db.models.sql import Query
from django.db.models import Q

from sesql import config


def make_add_filter(original):
    def sesql_amdin_query_add_filter(self, filter_expr, *args, **kwargs):
        from sesql import longquery
        name, value = filter_expr
        if not name.startswith("sesql:"):
            return original(self, filter_expr, *args, **kwargs)

        # Ok, a SeSQL filter ? Hum hum
        name = name.split(':', 1)[1]
        if "__" in name:
            name = name.split('__', 1)[0]
        name += "__containswords"
        query = longquery.longquery(Q(classname = self.model) &
                                    Q(**{ name: value }))
        ids = [ oid for klass, oid in query.objs ]
        return original(self, ('id__in', ids), *args, **kwargs)

    return sesql_amdin_query_add_filter

if getattr(config, 'ENABLE_SESQL_ADMIN', False):
    if not getattr(Query, "_sesql_admin_patch_applied", None):
        Query._sesql_admin_patch_applied = True
        Query.add_filter = make_add_filter(Query.add_filter)
