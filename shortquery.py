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

import sesql_config as config
from sesql.query import SeSQLQuery
from sesql import utils

@utils.log_time
def shortquery(query, order = None, limit = 50, historize=False):
    """
    Perform a short query and return a lazy Django result set
    """
    query = SeSQLQuery(query, order)
    results = query.shortquery(limit)

    if historize: #suggest feature hook
        results.historize(query)

    return results
