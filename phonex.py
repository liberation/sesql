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

def phonex(query):
    """soundex algorithm for french language"""
    # clean up the query
    for char in u" -.+*/,:;_'":
        query = query.replace(char, '')

    # source: http://www-lium.univ-lemans.fr/~carlier/recherche/soundex.html#L4
    # 1. replace all 'y' with 'i'

    query = query.replace('y', 'i')

    # 2. replace all 'h' that are not preceded by 'c', 's' or 'p'
    first_char = query[0]
    if first_char == 'h':
        output = ''
    else:
        output = first_char

    previous_char = first_char
    for current_char in query[1:]:
        if current_char == 'h':
            if previous_char in 'csp':
                # keep the 'h'
                output += current_char
        else:
            output += current_char
        previous_char = current_char
    query = output
            
    # 3. replace 'ph' by 'f'
    query = query.replace('ph', 'f')

    # 4. replace 

    query = query.replace('gan', 'kan')
    query = query.replace('gam', 'kam')
    query = query.replace('gain', 'kain')
    query = query.replace('gaim', 'kaim')

    # 5. replace if followed by 'a', 'e', 'i', 'o' or 'u'
    # 'ain' -> 'yn'
    # 'ein' -> 'yn'
    # 'aim' -> 'yn'
    # 'eim' -> 'yn'
    
    N = len(query)
    index = 0
    output = ''
    while N-index>=3:
        i0_char = query[index]
        if i0_char in 'ae':
            i1_char = query[index+1]
            if i1_char == 'i':
                i2_char = query[index+2]
                if i2_char in 'mn':
                    try:
                        i3_char = query[index+3]
                    except:
                        i3_char = None
                    if i3_char and i3_char in 'aeiou':
                        output += 'yn'
                        index += 3
                        continue
        output += i0_char
        index += 1

    query = output + query[index-N:]

    # 6. replace 'o', 'oua' and 'ein' sound

    query = query.replace('eau', 'o')
    query = query.replace('oua', '2')
    query = query.replace('ein', '4')
    query = query.replace('ain', '4')
    query = query.replace('eim', '4')
    query = query.replace('aim', '4')

    # 7. replace 'é'

    query = query.replace(u'é', 'y')
    query = query.replace(u'è', 'y')
    query = query.replace(u'ê', 'y')
    query = query.replace('ai', 'y')
    query = query.replace('ei', 'y')
    query = query.replace('er', 'yr')
    query = query.replace('ess', 'yss')
    query = query.replace('et', 'yt')

    # 8. replace 'an' and 'in', 
    # except if it's followed by 'a', 'e', 'i', 'o', 'u' 
    # or a sound between 1 and 4

    N = len(query)
    index = 0
    output = ''

    while N-index>=2:
        i0_char = query[index]
        if i0_char in 'ae':
            i1_char = query[index+1]
            if i1_char in 'nm':
                try:
                    i2_char = query[index+2]
                except:
                    i2_char = None
                if i2_char:
                    if not i2_char in 'aeiou1234':
                        output += '1'
                        index += 2
                        continue
                else:
                    output += '1'
                    index += 2
                    continue                        
        elif i0_char == 'i':
            i1_char = query[index+1]
            if i1_char == 'n':
                try:
                    i2_char = query[index+2]
                except:
                    i2_char = None
                if i2_char:
                    if not i2_char in 'aeiou1234':
                        output += '4'
                        index += 2
                        continue
                else:
                    output += '4'
                    index += 2
                    continue                        
        output += i0_char
        index += 1
    delta = index-N
    if delta == 0:
        query = output
    else:
        query = output + query[delta:]

    # 8bis. replace 'on' by '1'
        
    query = query.replace('on', '1')

    # 9. replace s by z if they are followed 
    # and preceded by 'a', 'e', 'i', 'o', 'u' 
    # or a sound from 1 to 4
    
    N = len(query)
    if N > 2:
        output = query[0]
        for index in [e+1 for e in xrange(len(query)-2)]:
            char = query[index]
            if char == 's':
                previous = query[index-1]
                next_ = query[index+1]

                if previous in 'aeiou1234' and next_ in  'aeiou1234':
                    output += 'z'
                    continue
            output += char
        output += query[-1]
        query = output 

    # 10. replace 

    query = query.replace('oe', 'e')
    query = query.replace('eu', 'e')
    query = query.replace('au', 'o')
    query = query.replace('oi', '2')
    query = query.replace('oy', '2')
    query = query.replace('ou', '3')

    # 11.

    query = query.replace('ch', '5')
    query = query.replace('sch', '5')
    query = query.replace('sh', '5')
    query = query.replace('ss', 's')
    query = query.replace('sc', 's')



    # 12. replace 'c' by an 's' if it's followed by an 'e' or an 'i'
    output = ''
    for index in range(len(query)-1):
        char = query[index]
        if char == 'c':
            next_ = query[index+1]
            if next_ in 'ei':
                output += 's'
                continue
        output += char
    output += query[-1]
    query = output

    # 13. replace

    query = query.replace('c', 'k')
    query = query.replace('q', 'k')
    query = query.replace('qu', 'k')
    query = query.replace('gu', 'k')
    query = query.replace('ga', 'ka')
    query = query.replace('go', 'ko')
    query = query.replace('gy', 'ky')

    # 14. replace

    query = query.replace('a', 'o')
    query = query.replace('d', 't')
    query = query.replace('p', 't')
    query = query.replace('j', 'g')
    query = query.replace('b', 'f')
    query = query.replace('v', 'f')
    query = query.replace('m', 'n')

    # 15. replace duplicate letters

    output = ''

    for index in range(len(query)):
        char = query[index]
        try:
            next_ = query[index+1]
        except:
            next_ = False
        if next_ and char == next_:
            continue
        output += char
    query = output

    # 16. remove 't' & 'x' end

    if query[-1] in 'tx':
        query = query[:-1]

    # 17. replace

    query = list(query)

    REPLACE = dict()
    REPLACE['1'] = 0
    REPLACE['2'] = 1
    REPLACE['3'] = 2
    REPLACE['4'] = 3
    REPLACE['5'] = 4
    REPLACE['e'] = 5
    REPLACE['f'] = 6
    REPLACE['g'] = 7
    REPLACE['h'] = 8
    REPLACE['i'] = 9
    REPLACE['k'] = 10
    REPLACE['l'] = 11
    REPLACE['n'] = 12
    REPLACE['o'] = 13
    REPLACE['r'] = 14
    REPLACE['s'] = 15
    REPLACE['t'] = 16
    REPLACE['u'] = 17
    REPLACE['w'] = 18
    REPLACE['x'] = 19
    REPLACE['y'] = 20
    REPLACE['z'] = 21

    value = 0

    for index in range(len(query)):
        v = REPLACE[query[index]]
        value += v*22**-(index+1)

    return value
