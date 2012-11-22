# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010-2012

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

import time
import logging
log = logging.getLogger('sesql')


class Timer(object):
    """
    A timer object to be used with « with » statement
    It as a local and global timer
    """
    def __init__(self):
        self._local = self._global = 0.0
        self._start = time.time()

    def start(self):
        self._start = time.time()

    def stop(self):
        delta = time.time() - self._start
        self._local += delta
        self._global += delta

    def __enter__(self):
        self.start()

    def __exit__(self, *args, **kwargs):
        self.stop()

    def get_local(self):
        return self._local

    def get_global(self):
        return self._global

    def reset(self):
        self._local = 0

    def peek(self):
        res = self.get_local()
        self.reset()
        return res

def safe_str(what):
    """
    Gives a safe string form of what, never failing
    """
    try:
        return str(what)
    except:
        try:
            return repr(what)
        except:
            return '<Broken object>'

def log_time(function, message = None):
    """
    Decorator to log function call and execution time
    """
    tmr = Timer()
    def log_time_inner(*args, **kwargs):
        tmr.__enter__()
        try:
            res = function(*args, **kwargs)
        finally:
            tmr.__exit__()
        m = message
        if m is None:
            args = ', '.join([ safe_str(a) for a in args ])
            extra = [ "%s=%s" % (key, value) for key, value in kwargs.items() ]
            kwargs = ', '.join(extra)
            m = '%s (%s, %s)' % (function.__name__, args, kwargs,)
        log.info('%s : %.2f second(s)' % (m, tmr.peek()))
        return res
    log_time_inner.__name__ = function.__name__
    return log_time_inner

                 
def strip_ligatures(value):
    """
    Convert the ligatures (œ, æ, ß, ...) into their compound value (oe, ae, ss)
    Take **unicode** as input and output, not string
    """
    charmap = {
        u'\N{Latin capital letter AE}': 'AE',
        u'\N{Latin small letter ae}': 'ae',
        u'\N{Latin capital letter Eth}': 'Dh',
        u'\N{Latin small letter eth}': 'dh',
        u'\N{Latin capital letter O with stroke}': 'Oe',
        u'\N{Latin small letter o with stroke}': 'oe',
        u'\N{Latin capital letter Thorn}': 'Th',
        u'\N{Latin small letter thorn}': 'th',
        u'\N{Latin small letter sharp s}': 'ss',
        u'\N{Latin capital letter D with stroke}': 'Dj',
        u'\N{Latin small letter d with stroke}': 'dj',
        u'\N{Latin capital letter H with stroke}': 'H',
        u'\N{Latin small letter h with stroke}': 'h',
        u'\N{Latin small letter dotless i}': 'i',
        u'\N{Latin small letter kra}': 'q',
        u'\N{Latin capital letter L with stroke}': 'L',
        u'\N{Latin small letter l with stroke}': 'l',
        u'\N{Latin capital letter Eng}': 'Ng',
        u'\N{Latin small letter eng}': 'ng',
        u'\N{Latin capital ligature OE}': 'Oe',
        u'\N{Latin small ligature oe}': 'oe',
        u'\N{Latin capital letter T with stroke}': 'Th',
        u'\N{Latin small letter t with stroke}': 'th',
    }

    value = ''.join([ charmap.get(c,c) for c in value ])    
    return value

def format_time(timedelta, keep = 3):
    """
    Pretty format a timedelta
    """
    units = [ 'ms', 's', 'm', 'h', 'd' ]
    factors = [ 1000, 60, 60, 24, None ]
    formats = [ '%03d', '%02d', '%02d', '%02d', '%d' ]

    res = []
    value = int(timedelta * factors[0])
    for factor, unit, fmt in zip(factors, units, formats):
        if factor:
            nextval = value / float(factor)
            val = nextval - int(nextval)
            val = int(val * factor)
            value = int(nextval)
        else:
            val = value
        res.append((val, fmt, unit))

    while res and res[-1][0] == 0:
        res.pop()

    res = res[-3:]
    res = [ r for r in res if r[0] ]
    res = res[::-1]
    if res:
        res = ', '.join([ fmt % val + ' ' + unit for val, fmt, unit in res ])
    else:
        res = '0'

    res = '%.2f seconds (%s)' % (timedelta, res)
    return res
        
    
def print_eta(percent, timedelta):
    """
    Print an ETA line, given we did percent of total in timedelta
    """
    if percent == 100.0 or percent == 0.0:
        eta = 0
    else:
        eta = timedelta / percent * (100.0 - percent)

    print "%05.2f %% done in %s; ETA : %s" % (percent,
                                              format_time(timedelta),
                                              format_time(eta))
