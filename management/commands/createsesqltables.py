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

from django.core.management.base import BaseCommand
from django.core.management import call_command
import settings
from sesql import datamodel
from sesql.typemap import typemap

class Command(BaseCommand):
    help = "Dump the commands to create SeSQL tables"
    
    def handle(self, *apps, **options):
        """
        Handle the command
        """       
        print "BEGIN;"

        datamodel.create_dictionnary(include_drop = True)
        datamodel.create_master_table(include_drop = True)
        
        for table in typemap.all_tables():
            datamodel.create_table(table = table, include_drop = True)
            
        datamodel.create_schedule_table(include_drop = True)

        print "COMMIT;"
