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

"""Command line parser, should be converted to argparse ?"""

import sys, getopt

class CmdLine(object):
    "The command line parser"
    def __init__(self, argv):
        self.argv = argv
        self.all = []
        self.longs = []
        self.shorts = ""
        self.convert = {}
        self.values = {}
        self.add_opt("help", "h",
                     help_msg = "display this help")
        self.add_opt("version", "v",
                     help_msg = "display version number and exits")

    def __getitem__(self, item):
        return self.values[item]

    def __setitem__(self, item, value):
        self.values[item] = value

    def has_key(self, key):
        return self.values.has_key(key)

    def items(self):
        return self.values.items()

    def add_opt(self, long, short = "", value = None, help_msg = ""):
        "Adds an option to the list of known ones"
        self.all.append((long, short, value, help_msg))
        self.values[long] = value
        self.convert["--" + long] = long
        if(short):
            self.convert["-" + short] = long
            self.shorts = self.shorts + short
        if(not(value is None)):
            self.longs.append(long + "=")
            if(short):
                self.shorts = self.shorts + ":"
        else:
            self.longs.append(long)
            
    def parse_opt(self):
        "Parse the command line"
        try:
            optlist, args = getopt.getopt(self.argv[1:], self.shorts, self.longs)
        except getopt.GetoptError, s:
            print self.argv[0] + ":", s, ". Try --help."
            sys.exit(2)

        self.args = args
        for opt, val in optlist:
            # Extra key from options
            while(self.convert.has_key(opt)):
                opt = self.convert[opt]
            if(val):
                self.values[opt] = val
            else:
                self.values[opt] = True
        
    def show_help(self, extra = ""):
        print "Syntax: %s %s [<options>]" % (self.argv[0], extra)
        print "Options:"
        longest = max([ len(l) for l in self.convert.keys() ])
        for long, short, value, help_msg in self.all:
            default = value and "(default: %s)" % value or ""
            name = "--" + long
            name += " " * (longest - len(name))
            if short:
                name += ", -" + short
            else:
                name += "    "
            print "  %s: %s %s" % (name, help_msg, default)
            
            
