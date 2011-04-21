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

from optparse import make_option
from datetime import datetime
from datetime import timedelta 

from django.core.management.base import BaseCommand

import settings

from sesql.lemmatize import lemmatize
from sesql.models import SearchHit
from sesql.models import SearchQuery
from sesql.models import SearchHitHistoric
from sesql.suggest import phonex

import sesql_config as config


class Command(BaseCommand):
    help = """Perform a SearchQuery update based on last week searches"""
    
    option_list = BaseCommand.option_list + (
        make_option('-e','--erode',
                    action='store_true',
                    dest='erode',
                    help = 'tell if we must erode result or not'),
            
        make_option('-f','--filter', #FIXME: not supported
                    dest ='filter',
                    type='int',
                    default=config.DEFAULT_FILTER,
                    help = 'how many time a search must occur to be treated'))
    
    def handle(self, *apps, **options):
        self.process_hits(options['filter'])
        
        if options['erode']:
            self.erode()

    def erode(self):
        for search_query in SearchQuery.objects.all():
            search_query.pondered_search_nb = (config.ALPHA 
                                               * search_query.pondered_search_nb 
                                               + (1-config.ALPHA)
                                               * search_query.nb_recent_search)
            search_query.nb_recent_search = 0
            search_query.save()
        
    def process_hits(self, filter_nb):
        last_hits = SearchHit.objects.all().order_by('-date')

        processed_hits = []

        for hit in last_hits:
            query = hit.query
            
            # blacklist
            if query in config.BLACKLIST:
                continue

            # filter
            # if there is not enought hit and 
            # the query is not already in db
            # we don't process it
            if SearchHit.objects.all().filter(query=query).count() < filter_nb:
                # not enough hit 
                if SearchQuery.objects.filter(query=query).count() == 0:
                     # not already in db
                    continue 
            # get or create SearchQuery object based on query
            try:
                search_query = SearchQuery.objects.get(query=query)
                created = False
            except:
                search_query = SearchQuery(query=query)
                created = True

            # if it's a new one, initialize it
            if created:
                search_query.phonex = phonex(query)

                lems = lemmatize(query.split())
                clean_query = []
                for lem in lems: # select only strings values 
                                 # and not empty strings''
                    if lem:
                        clean_query.append(lem)

                clean_query = ' '.join(clean_query)
                clean_phonex = phonex(clean_query)

                search_query.clean_query = clean_query
                search_query.clean_phonex = clean_phonex

                search_query.nb_total_search = 0
                search_query.pondered_search_nb = 0
                search_query.nb_recent_search = 0

            search_query.nb_results = hit.nb_results
            search_query.nb_total_search += 1

            search_query.pondered_search_nb += 1
            search_query.nb_recent_search += 1 

            search_query.weight = (search_query.pondered_search_nb * config.BETA + 
                                   search_query.nb_results * config.GAMMA)
            search_query.save()

            # we can now create SearchHitHistoric 
            SearchHitHistoric(query=hit.query,
                              nb_results=hit.nb_results,
                              date=hit.date).save()
            processed_hits.append(hit)
    
        for hit in processed_hits:
            hit.delete()
