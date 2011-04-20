from django.db import models



class SearchHit(models.Model):
    """Used to store queries made to the search engine"""
    query = models.CharField(max_length=255)
    nb_results = models.PositiveIntegerField()
    date = models.DateField(auto_now=True)


class SearchHitHistoric(models.Model):
    """Same as SearchHit used as an archive"""
    query = models.CharField(max_length=255)
    nb_results = models.PositiveIntegerField()
    date = models.DateField(auto_now=True, db_index=True)
   

class SearchQuery(models.Model):
    query = models.CharField(max_length=255)
    phonex = models.FloatField()
    clean_query = models.CharField(max_length=255)
    clean_phonex = models.FloatField(max_length=255)
    
    nb_results = models.PositiveIntegerField()
    
    nb_recent_search = models.PositiveIntegerField()
    nb_total_search = models.PositiveIntegerField()
    pondered_search_nb = models.FloatField()
    
    weight = models.FloatField()
