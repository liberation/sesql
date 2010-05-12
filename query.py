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

from django.db.models import Q
from sesql.typemap import typemap
from sesql.fieldmap import fieldmap
import sesql_config as config

from django.db import connection

import logging
log = logging.getLogger('sesql')

from sesql.results import SeSQLResultSet

def cached(method):
    """
    Decorator to make a method without argument to store result
    in the object itself
    """
    cache_name = "_cache_" + method.__name__
    def cached_inner(self, *args, **kwargs):
        if args or kwargs:
            return method(self, *args, **kwargs)
        
        if not hasattr(self, cache_name):
            value = method(self)
            setattr(self, cache_name, value)
        else:
            value = getattr(self, cache_name)
        return value
    return cached_inner

class SeSQLQuery(object):
    """
    SeSQL Query handler
    """
    def __init__(self, query, order):
        """
        Constructor
        """
        self.query = query
        order = order or config.DEFAULT_ORDER
        if isinstance(order, (str, unicode)):
            order = order.split(',')
        self.order = order

    def execute(self, query, values):
        """
        Execute and log query
        """
        cursor = connection.cursor()
        log.debug("Query %r with values %r" % (query, values))
        cursor.execute(query, values)
        return cursor

    def longquery(self, limit = None):
        """
        Perform a long query and return a lazy Django result set
        """
        return SeSQLResultSet(list(self._do_longquery(limit)))

    def _do_longquery(self, limit = None):
        """
        Perform a long query and return a cursor
        """
        table = self.get_table_name()
        pattern, values = self.get_pattern()
        o_pattern, o_values = self.get_order()

        query = """SELECT classname, id
FROM %s
WHERE %s
ORDER BY %s""" % (table, pattern, o_pattern)
        if limit:
            query += """
LIMIT %d""" % limit

        return self.execute(query, values + o_values)

    @cached
    def _get_smart_query(self):
        """
        Get the template for performing smart queries
        """
        table = self.get_table_name()
        pattern, values = self.get_pattern()
        o_pattern, o_values = self.get_order()

        classes = self.get_classes()
        l_pattern, l_values = fieldmap.get_field("classname").get_in(classes)
        l_order, _ = self.get_order(limit = 1)

        cursor = connection.cursor()

        #
        # We need to do some query optimization, since postgresql will not
        # handle well the cases of many matches
        #

        smartquery = """SELECT classname, id
FROM  (SELECT * FROM %s WHERE %s ORDER BY %s LIMIT {SESQL_SMART_LIMIT}) subquery
WHERE %s ORDER BY %s LIMIT {SESQL_THE_LIMIT}""" % (table, l_pattern, l_order, pattern, o_pattern)
        return smartquery, l_values + values + o_values
        
    def _attempt_short_query(self, size, limit):
        """
        Attempt a short query of given size
        """
        smartquery, values = self._get_smart_query()
        query = smartquery.replace('{SESQL_SMART_LIMIT}', str(size))
        query = query.replace('{SESQL_THE_LIMIT}', str(limit))
        return self.execute(query, values)

    def _do_smart_query(self, limit):
        """
        Perform a smart query, returning cursor
        """
        # Ok we can do a short query
        cursor = self._attempt_short_query(config.SMART_QUERY_INITIAL, limit)
        if cursor.rowcount >= limit:
            log.debug("Found data with Query Plan A")
            return cursor

        log.debug("Found %d/%d rows with Query Plan A" % (cursor.rowcount,
                                                          limit))
        if cursor.rowcount >= limit * config.SMART_QUERY_THRESOLD:
            log.debug("Trying Query Plan B")
            # Not enough, but promising, let's try again
            ratio = float(limit) / float(cursor.rowcount)
            ratio *= config.SMART_QUERY_RATIO
            sl = int(config.SMART_QUERY_INITIAL * ratio)
            cursor = self._attempt_short_query(sl, limit)
            if cursor.rowcount >= limit:
                log.debug("Found data with Query Plan B")
                return cursor
            log.debug("Found %d/%d rows with Query Plan B" % (cursor.rowcount,
                                                              limit))
        log.debug("Using Query Plan C")
        return self._do_longquery(limit)

    def shortquery(self, limit = 50):
        """
        Perform a long query and return a lazy Django result set
        """
        table = self.get_table_name()

        if table == config.MASTER_TABLE_NAME:
            # Multitable or unprecise query ? Falling back to longquery
            log.warning("Query on master table will not be optimized on %s" % self.query)
            return self.longquery(limit)

        if "sesql_relevance"  in self.order or "-sesql_relevance" in self.order:
            # Order on relevance ? Falling back to longquery
            log.info("Query sorting on relevance will not be optimized on %s" % self.query)
            return self.longquery(limit)

        log.debug("Trying short query for %s" % self.query)

        cursor = self._do_smart_query(limit)
        return SeSQLResultSet(list(cursor))

    @cached
    def get_table_name(self):
        """
        Get the name of table to use for the query
        For now, if we are accross more than one, use the master table
        """
        classes = self.get_classes()
        tables = set()
        for k in classes:
            tables.add(typemap.get_table_for(k))
        if len(tables) != 1:
            log.warning("Can't find a single table to look at, using master table, in query %s" % self.query)
            return config.MASTER_TABLE_NAME
        return tables.pop()

    @cached
    def get_classes(self):
        """
        Get the name of classes involved in the query
        """
        node = self._find_node_for(self.query, "classname")
        if not node:
            return []
        key, value = node
        if key == "classname":
            return [ value ]
        if key == "classname__in":
            return value
        return []

    def _find_node_for(self, node, field):
        """
        Find a node for field 'field' starting from node 'node'
        """
        if isinstance(node, Q):
            if node.negated:
                log.debug("Found a negated node, stop looking for %s" % field)
                return None
            if node.connector != 'AND':
                log.debug("Found a OR node, stop looking for %s" % field)
                return None
            for child in node.children:
                what = self._find_node_for(child, field)
                if what:
                    return what
            return None

        key, value = node
        if key == field or key.startswith(field + '__'):
            return node

    @cached
    def get_pattern(self):
        """
        Get the SQL pattern of the query, as tuple (pattern, values)
        """
        return self.get_pattern_for(self.query)

    def get_pattern_for(self, node):
        """
        Get the SQL pattern of the query, as tuple (pattern, values)
        """
        if isinstance(node, Q):
            if node.negated:
                node.negated = False
                pattern, values = self.get_pattern_for(node)
                node.negated = True
                pattern = "NOT (%s)" % pattern
                return pattern, values

            patterns = []
            values = []
            for child in node.children:
                pattern, vals = self.get_pattern_for(child)
                values.extend(list(vals))
                patterns.append(pattern)

            connector = ') %s (' % node.connector
                
            pattern = '(' + connector.join(patterns) + ')'
            return pattern, values
                
        query, value = node
        if "__" in query:
            field, method = query.split("__", 1)
        else:
            field = query
            method = "default"

        field = fieldmap.get_field(field)        
        method = getattr(field, "get_" + method)
        return method(value)
    
    @cached
    def get_order(self, limit = None):
        """
        Get the order by clause
        """
        res = []
        if limit:
            order = self.order[:limit]
        else:
            order = self.order            

        values = []
            
        for o in order:
            if o[0] == '-':
                o = o[1:]
                direction = "DESC"
            else:
                direction = "ASC"

            if o == "sesql_relevance":
                query = self.get_fulltext_query()
                if not query:
                    log.warning("No full text query, ignoring relevance in %s" % self.query)
                    continue
                field, what, value = query
                o = "ts_rank_cd(%s, %s)" % (field, what)
                values.extend(value)
                
            res.append(o + " " + direction)
        return ','.join(res), values
    
    @cached
    def get_fulltext_query(self):
        """
        Get the fulltext query as
        field = name of the field
        what = type of search (plainto_tsquery or just to_tsquery)
        value = value of the search
        Return None if none found
        """
        field = fieldmap.get_primary()
        if not field:
            log.warning("No primary field defined, ignoring ranking")
        node = self._find_node_for(self.query, field.name)
        if not node:
            return None
        key, value = node
        if "__" in key:
            key, method = key.split("__")
        else:
            method = "default"

        method = getattr(field, "rank_" + method)
        return method(value)
    
