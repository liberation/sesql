# -*- coding: utf-8 -*-
from optparse import make_option
from datetime import datetime
from datetime import timedelta 

from django.core.management.base import BaseCommand

import settings

from sesql.lemmatize import lemmatize
from sesql.models import SearchHit, SearchQuery
from sesql.suggest import is_blacklisted, phonex



ALPHA = 0.95 #erode factor
BETA = 1 #used to compute weight
GAMMA = 1 #user to compute weight

class Command(BaseCommand):
    help = """Perform a SearchQuery update based on last week searches"""
    
    option_list = BaseCommand.option_list + (
        make_option('-e','--erode',
                    action='store_true',
                    dest='erode',
                    help = 'tell if we must erode result or not'),
            
        make_option('-f','--filter',
                    dest ='filter',
                    type='int',
                    default=5,
                    help = 'how many time a search must occur to be treated'))
    
    def handle(self, *apps, **options): #FIXME: we do not support the options
        now = datetime.now()
        last_week = now - timedelta(days=7)

        last_week_hits = SearchHit.objects.filter(date__lte=now).filter(date__gte=last_week).order_by('date')
        last_week_queries = last_week_hits.values_list('query', flat=True).distinct()
        for query in last_week_queries:
            if not is_blacklisted(query):
                try:
                    search_query = SearchQuery.objects.get(query=query)
                    created = False
                except:
                    search_query = SearchQuery(query=query)
                    created = True

                if created: # setup the search_query
                    search_query.phonex = phonex(query)

                    lems = lemmatize(query.split())
                    clean_query = []
                    for lem in lems:
                        if lem:
                            clean_query.append(lem)

                    clean_query = ' '.join(clean_query)
                    clean_phonex = phonex(clean_query)

                    search_query.clean_query = clean_query
                    search_query.clean_phonex = clean_phonex

                    search_query.nb_total_search = 0
                    search_query.pondered_search_nb = 0

                search_query.nb_results = SearchHit.objects.filter(query=query).order_by('-date')[0].nb_results
                search_query.nb_recent_search = last_week_hits.filter(query=query).count()
                search_query.nb_total_search += search_query.nb_recent_search

                search_query.pondered_search_nb = ALPHA * search_query.pondered_search_nb + (1-ALPHA)* search_query.nb_recent_search
                

                search_query.weight = (search_query.pondered_search_nb * BETA + 
                                       search_query.nb_results * GAMMA)
                search_query.save()
