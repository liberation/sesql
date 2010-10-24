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

from sesql.lemmatize import lemmatize
from sesql.fieldmap import fieldmap
import string

def highlight(text, words, index = None):
    """
    Give the position of words in a text
    That can be used to highlight the words, for example
    The index will be use to lemmatize, if none, it'll use the default one
    """
    if index is None:
        index = fieldmap.primary

    if index is None:
        raise ValueError, "Not index given and no primary one"

    size = len(text)
    letters = set(string.ascii_letters)
    
    # Lemmatize the words
    lems = lemmatize(words, index)

    # Marshall everything
    text = index.marshall(text, use_cleanup = False)

    # Now find the lemmatized words inside the text
    found = []
    foundwords = set()
    for i, lem in enumerate(lems):
        if not lem:
            continue
        wordsize = len(lem)
        pos = 0
        while True:
            begin = text.find(lem, pos)
            if begin < 0:
                break
            end = begin + wordsize

            # We found something, ensure it's a normal word
            if begin and text[begin - 1] in letters:
                pos = end
                continue

            # Now find the end of the word
            while end < size and text[end] in letters:
                end += 1

            found.append((begin, end, i))
            foundwords.add(text[begin:end])
            pos = end

    # Lemmatize all found words
    foundwords = list(foundwords)
    foundlems = lemmatize(foundwords, index)
    foundlems = dict(zip(foundwords, foundlems))

    # And now, second pass, ensure lemmatized version of word is word
    results = []
    for begin, end, i in found:
        word = text[begin:end]
        lem = foundlems[word]
        wanted_lem = lems[i]
        if lem == wanted_lem:
            results.append((begin, end, i))

    return results
        
            
        
    
