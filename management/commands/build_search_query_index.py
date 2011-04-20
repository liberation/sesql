# -*- coding: utf-8 -*-
from optparse import make_option
from datetime import datetime
from datetime import timedelta 

from django.core.management.base import BaseCommand

import settings

from sesql.lemmatize import lemmatize
from sesql.models import SearchHit
from sesql.models import SearchQuery
from sesql.models import SearchHitHistoric
from sesql.suggest import is_blacklisted, phonex

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
            search_query.pondered_search_nb = (config.ALPHA * search_query.pondered_search_nb 
                                               + (1-config.ALPHA)* search_query.nb_recent_search)
        
    def process_hits(self, filter_nb):
        last_hits = SearchHit.objects.all().order_by('-date')
        for hit in last_hits:
            query = hit.query
            if not is_blacklisted(query):
                # get or create SearchQuery object based on query
                try:
                    search_query = SearchQuery.objects.get(query=query)
                    created = False
                except:
                    search_query = SearchQuery(query=query)
                    created = True

                # if it's a new one, initialise it
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
                hit.delete()
        
