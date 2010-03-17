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
Contain the field types for SeSQL
We cannot reuse Django types because what we need is too specific
"""

import unicodedata, locale
from htmlentitydefs import name2codepoint
from xml.sax import saxutils
from sources import guess_source, ClassSource
import config

html_entities = dict([('&%s;' % k, unichr(v).encode(config.CHARSET)) for k,v in name2codepoint.items() ])

class Field(object):
    """
    This represent an abstract field
    """
    primary = False
    slqtype = None
    indexfunction = ""
    placeholder = "%s"
    
    def __init__(self, name, source = None, sql_default = None):
        """
        Constructor
        name = name for the field in our database
        sources = list of names used in input
        """
        self.name = name
        self.index_column = name
        self.sql_default = sql_default
        self.data_column = name
        if not source:
            source = name
        self.source = guess_source(source)

    def schema(self):
        """
        Get the field definition
        """
        schema = "%s %s" % (self.name, self.sqltype)
        if self.sql_default:
            schema += " DEFAULT %s" % self.sql_default
        return schema + ","

    def index(self, tablename):
        """
        Get the index defintion
        """
        index = "CREATE INDEX %s_%s_index ON %s " % (tablename, self.name, tablename)
        if self.indexfunction:
            index += "USING %s " % self.indexfunction
        index = index + "(%s);" % self.index_column
        return index

    def get_value(self, obj):
        """
        Get value from Django object
        For normal types, we can only have one value
        """
        return self.marshall(self.source.load_data(obj))

    def marshall(self, value):
        """
        Marshall the value to SQL
        """
        if not value:
            return None
        if isinstance(value, unicode):
            return value.encode(config.CHARSET, 'ignore')
        return str(value)

    def get_default(self, value):
        """
        Get the default pattern
        """
        return self.index_column + ' = %s', [ self.marshall(value) ]

    def get_in(self, value):
        """
        Get the pattern for __in operator
        """
        if not isinstance(value, (list, tuple)):
            raise ValueError, "__in requires a list or tuple"

        value = [ self.marshall(val) for val in value ]
        patt = [ "%s" for val in value ]
        return self.index_column + " IN (" + ",".join(patt) + ")", value

class IntField(Field):
    """
    This is a single integer field
    """
    sqltype = "integer"

    def marshall(self, value):
        """
        Marshall the value to SQL
        """
        if not value:
            return None
        return int(value)

    def get_lt(self, value):
        """
        Get the __lt pattern
        """
        return self.index_column + ' < %s', [ self.marshall(value) ]

    def get_lte(self, value):
        """
        Get the __lt pattern
        """
        return self.index_column + ' <= %s', [ self.marshall(value) ]

    def get_gt(self, value):
        """
        Get the __lt pattern
        """
        return self.index_column + ' > %s', [ self.marshall(value) ]

    def get_gte(self, value):
        """
        Get the __lt pattern
        """
        return self.index_column + ' >= %s', [ self.marshall(value) ]

    def get_range(self, value):
        """
        Get the __range pattern
        """
        if not isinstance(value, (list, tuple)) or not len(value) == 2:
            raise ValueError, "__range requires a couple as parameters"

        pattern = '(%s >= %%s) AND (%s <= %%s)' % (self.index_column,
                                                   self.index_column)
        values = [ self.marshall(value[0]), self.marshall(value[1]) ]

        return pattern, values

class StrField(Field):
    """
    This is a simple string field, with specified length
    """
    def __init__(self, name, source = None, size = 255):
        """
        Constructor
        Takes one extra paramater: the field size
        """
        super(StrField, self).__init__(name, source)
        self.size = size
        self.sqltype = "varchar(%d)" % size

class ClassField(Field):
    """
    This is a field storing the class of the object
    """
    sqltype = "varchar(255)"
    
    def __init__(self, name):
        """
        Constructor
        """
        super(ClassField, self).__init__(name, None)
        self.source = ClassSource()

    def marshall(self, value):
        """
        Marshall the value to SQL
        """
        if hasattr(value, "__name__"):
            value = value.__name__
        return value

class DateField(IntField):
    """
    This a date only field, inherit from IntField so we get all the < > <= ...
    """
    sqltype = "date"

    def marshall(self, value):
        """
        Marshall a date field
        """
        if not value:
            return None
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return value and str(value) or None

class DateTimeField(DateField):
    """
    This a date and time field
    """
    sqltype = "timestamp"

    def marshall(self, value):
        """
        Marshall a date field
        """
        if not value:
            return None
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S %z")
        return value and str(value) or None

class IntArrayField(Field):
    """
    This is an array of integer
    """
    sqltype = "integer[]"
    indexfunction = "GIN"

    def marshall(self, value):
        """
        Marshall the values to SQL - input must be a list or tuple
        """
        if not value:
            return None
        if isinstance(value, (list, tuple)):
            return "{" + ",".join([ str(v) for v in value if v ]) + "}"
        else:
            return str(value)

    def operator(self, operator, values):
        """
        Get a SQL expression for this operator
        """
        pattern = "%s %s %%s" % (self.index_column, operator)
        values = self.marshall(values)
        return pattern, values

    def get_default(self, value):
        """
        Get the default pattern
        """
        return self.operator("@>", [ value ])
    
    def get_in(self, value):
        """
        Get the pattern for __in operator
        """
        raise ValueError, " __in = not supported for IntArrayField"

    def get_all(self, value):
        """
        Get the pattern for __all operator
        """
        if not isinstance(value, (list, tuple)):
            raise ValueError, "__all requires a list or tuple"

        return self.operator("@>", value)
    
    def get_any(self, value):
        """
        Get the pattern for __any operator
        """
        if not isinstance(value, (list, tuple)):
            raise ValueError, "__any requires a list or tuple"

        return self.operator("&&", value)


class FullTextField(Field):
    """
    This is a full text field
    """
    indexfunction = "GIN"
    dictionnary = "public.simple_french"

    def __init__(self, name, source = None, primary = False):
        """
        Constructor
        """
        super(FullTextField, self).__init__(name, source)
        self.index_column = name + "_tsv"
        self.data_column = name + "_text"
        self.primary = primary

    def marshall(self, value, extra_letters = ""):
        """
        Strip accents, escape html_entities, handle unicode, ...
        """
        if not value:
            return

        if isinstance(value, unicode):
            value = value.encode(config.CHARSET)
            
        value = saxutils.unescape(value, html_entities)

        if not isinstance(value, unicode):
            try:
                value = value.decode(config.CHARSET)
            except UnicodeDecodeError:
                raise ValueError, "Can't parse %s in %s" % (value, config.CHARSET)

        # Replace non-standard character by spaces
        def isletter(c):
            category = unicodedata.category(c)[0]
            return category in ('L', 'N') or c in extra_letters
        value = u''.join([ isletter(c) and c or u' ' for c in value ])

        # Now strip accents
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
        value = value.lower()
        return value

    def schema(self):
        """
        Get the field definition
        """
        return """%s text,
  %s tsvector,""" % (self.data_column, self.index_column)

    def index(self, tablename):
        """
        Get the index defintion
        """
        value = super(FullTextField, self).index(tablename)
        value += """
ALTER TABLE %s ALTER COLUMN %s SET STATISTICS 10000;
CREATE TRIGGER %s_%s_update BEFORE INSERT OR UPDATE
ON %s FOR EACH ROW EXECUTE PROCEDURE
tsvector_update_trigger(%s, '%s', %s);""" % (tablename, self.index_column, tablename, self.name, tablename, self.index_column, self.dictionnary, self.data_column)
        return value

    def get_default(self, value):
        """
        Get the default pattern
        """
        raise ValueError, " = not supported for FullTextField"
    
    def get_in(self, value):
        """
        Get the pattern for __in operator
        """
        raise ValueError, " __in = not supported for FullTextField"

    def pattern_contains(self, value):
        """
        Get the pattern for __contains* operators, in raw mode
        (return indexname, operator, value)
        """
        pattern = "plainto_tsquery('%s', %%s)" % self.dictionnary
        values = [ self.marshall(value) ]

        return self.index_column, pattern, values

    def pattern_matches(self, value):
        """
        Get the pattern for __matches operator, in raw mode
        (return indexname, operator, value)
        """
        pattern = "to_tsquery('%s', %%s)" % self.dictionnary
        values = [ self.marshall(value, extra_letters = '&|!()') ]

        return self.index_column, pattern, values

    def get_containswords(self, value):
        """
        Get the pattern for __containswords operator
        """
        column, pattern, values = self.pattern_contains(value)
        pattern = "%s @@ %s" % (column, pattern)
        return pattern, values
    
    def get_containsexact(self, value):
        """
        Get the pattern for __containsexact operator - can be slow.
        """
        pattern, values = self.get_containswords(what)
        pattern = "(%s AND %s)" % (pattern, "%s LIKE %%s" % self.data_column)
        values = [ values[0], '%' + values[0]  + '%' ]

        return pattern, values

    def get_matches(self, value):
        """
        Get the pattern for __matches operator (PostgreSQL tsquery string)
        """
        column, pattern, values = self.pattern_matches(value)
        pattern = "%s @@ %s" % (column, pattern)
        return pattern, values

    def get_like(self, value):
        """
        Get the pattern for __like operator - SLOW ! SLOW ! SLOW !
        """
        pattern = "%s LIKE %%s" % (self.index_column)
        values = [ self.marshall(what, extra_letters = '%') ]
        return pattern, values

    def rank_containswords(self, value):
        """
        Get the ranking pattern for __containswords operator
        """
        return self.pattern_contains(value)
    
    def rank_containsexact(self, value):
        """
        Get the ranking pattern for __containsexact operator - can be slow.
        """
        log.warning("Ranking on exact will fall back to ranking on contains")
        return self.rank_containswords(value)

    def rank_matches(self, value):
        """
        Get the ranking pattern for __matches operator
        """
        return self.pattern_matches(value)
