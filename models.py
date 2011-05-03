# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2011

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

from django.db import models

#
# Important note : the  core SeSQL tables are created in  raw SQL by a
# signal handler. Only the  suggest/history feature uses normal Django
# models.
#


class SearchHit(models.Model):
    """Used to store queries made to the search engine"""
    query = models.CharField(max_length=500)
    nb_results = models.PositiveIntegerField()
    date = models.DateField(auto_now=True)


class SearchHitHistoric(models.Model):
    """Same as SearchHit used as an archive"""
    query = models.CharField(max_length=500)
    nb_results = models.PositiveIntegerField()
    date = models.DateField(auto_now=True, db_index=True)
   

class SearchQuery(models.Model):
    """A table containing statistics and scores about search queries"""
    query = models.CharField(max_length=500)
    phonex = models.FloatField()
    clean_query = models.CharField(max_length=500)
    clean_phonex = models.FloatField()
    
    nb_results = models.PositiveIntegerField()
    
    nb_recent_search = models.PositiveIntegerField()
    nb_total_search = models.PositiveIntegerField()
    pondered_search_nb = models.FloatField()
    
    weight = models.FloatField()
