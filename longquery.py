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

from __future__ import with_statement

import string, random
from GenericCache import GenericCache

import sesql_config as config
from sesql.query import SeSQLQuery

import logging
log = logging.getLogger('sesql')

_query_cache = GenericCache(maxsize = config.QUERY_CACHE_MAX_SIZE,
                            expiry = config.QUERY_CACHE_EXPIRY)

def longquery(query, order = None, limit = None, queryid = None):
    """
    Perform a long query and return a lazy Django result set

    If queryid is provided, then the query will be loaded from the
    cache if possible, and redone else.

    Be careful, if the query is redone, results may have changed.
    """
    if queryid:
        with _query_cache.lock:
            results = _query_cache[queryid]
            if results:
                return results
            log.warning('Cached query id %r expired, re-querying.' % queryid)

    results = SeSQLQuery(query, order).longquery(limit)
        
    with _query_cache.lock:
        # Generate a new query id, ensuring it's unique
        while True:
            letters = string.ascii_letters + string.digits
            queryid = ''.join([ random.choice(letters) for i in range(32) ])
            if queryid not in _query_cache:
                break
        _query_cache[queryid] = results
        results.queryid = queryid
        
    return results
    
