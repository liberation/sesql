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

from sesql import config
from sesql.fieldmap import fieldmap

# Maximal number of words to lemmatize at once
MAX_WORDS = 1000

# Use GenericCache for now, but will probably be moved to memcached later
from GenericCache import GenericCache
_word_cache = GenericCache(maxsize = 50000, expiry = 86400)

def lemmatize_for(words, dictionnary):
    """
    Lemmatize a word with given dictionnary
    """
    if len(words) > MAX_WORDS:
        return lemmatize_for(words[:MAX_WORDS], dictionnary) + lemmatize_for(words[MAX_WORDS:], dictionnary)

    values = {}
    remaining = []

    for word in words:
        value = _word_cache[(word, dictionnary)]
        if value is not None:
            values[word] = value
        else:
            remaining.append(word)

    if remaining:
        pattern = "plainto_tsquery('%s', %%s)" % dictionnary
        patterns = [ pattern for word in remaining ]

        cursor = config.orm.cursor()
        cursor.execute('SELECT %s;' % (','.join(patterns)), remaining)
        row = cursor.fetchone()
        for word, value in zip(remaining, row):
            value = value.strip("'")
            values[word] = value
            _word_cache[(word, dictionnary)] = value

    return [ values[word] for word in words ]

def lemmatize(words, index = None):
    """
    Give a lemmatized version of those words

    Use the configuration for the given index, or the default one if
    index is None
    """
    if index is None:
        index = fieldmap.primary

    if index is None:
        raise ValueError, "Not index given and no primary one"

    words = [ index.marshall(word) for word in words ]

    index = fieldmap.get_field(index)
    return lemmatize_for(words, index.dictionnary)
