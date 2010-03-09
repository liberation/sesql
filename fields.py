# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
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
from sources import guess_source

CHARSET = "utf-8"

html_entities = dict([('&%s;' % k, unichr(v).encode(CHARSET)) for k,v in name2codepoint.items() ])

class Field(object):
    """
    This represent an abstract field
    """
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

    def get_value(self, context, row):
        """
        Get value from a context and a row 
        For normal types, we can only have one value
        """
        return self.marshall(self.source.load_data(context, row))

    def marshall(self, value):
        """
        Marshall the value to SQL
        """
        if not value:
            return None
        if isinstance(value, unicode):
            return value.encode(CHARSET, 'ignore')
        return str(value)

class IntField(Field):
    """
    This is a single integer field
    """
    sqltype = "integer"

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
        if isinstance(value, (list, tuple)):
            return "{" + ",".join([ str(v) for v in value if v ]) + "}"
        else:
            return str(value)

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

    def marshall(self, value):
        """
        Strip accents, escape html_entities, handle unicode, ...
        """
        if not value:
            return

        if isinstance(value, unicode):
            value = value.encode(CHARSET)
            
        value = saxutils.unescape(value, html_entities)

        if not isinstance(value, unicode):
            try:
                value = value.decode(CHARSET)
            except UnicodeDecodeError:
                raise ValueError, "Can't parse %s in %s" % (value, CHARSET)

        # Replace non-standard character by spaces
        def isletter(c): return unicodedata.category(c)[0] in ('L', 'N')
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

class DateField(Field):
    """
    This a date only field
    """
    sqltype = "date"

    def marshall(self, value):
        """
        Marshall a date field
        """
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return value and str(value) or None

