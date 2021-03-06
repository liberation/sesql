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

"""
This command will index or reindex a single object into SeSQL
Can be used as a test, or to fix a single problem
"""

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection, transaction
from django.db.models import Q
import settings
from sesql.results import SeSQLResultSet
from sesql.index import index
import sys

class Command(BaseCommand):
    help = "Index a single object into SeSQL"

    @transaction.commit_manually
    def handle(self, *apps, **options):
        """
        Handle the command
        """
        if len(apps) != 2:
            print "Syntax : sesqlindex <classname> <objid>"
            sys.exit(1)
        
        obj = SeSQLResultSet.load(apps)
        try:
            index(obj)        
            transaction.commit()
        except:
            transaction.rollback()
            raise
        
