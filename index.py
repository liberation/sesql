# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with SeSQL.  If not, see <http://www.gnu.org/licenses/>.

import config
from typemap import typemap
from django.db import connection

import logging
log = logging.getLogger('sesql')

def index(obj):
    """
    Index a Django object into SeSQL
    """
    cursor = connection.cursor()

    values = {}
    for field in config.FIELDS:
        values[field.name] = field.get_value(obj)
    table_name = typemap.get_table_for(obj.__class__)

    query = "DELETE FROM %s WHERE id=%%s AND classname=%%s" % table_name
    cursor.execute(query, (values["id"], values["classname"]))

    entry = "%s:%s" % (values["classname"], values["id"])
    
    if config.SKIP_CONDITION and config.SKIP_CONDITION(values):
        log.info("Deleting entry %s from table %s" % (entry, table_name))
        return
    
    log.info("Indexing entry %s in table %s" % (entry, table_name))
    
    keys = [  ]
    results = [  ]
    placeholders = [  ]

    for field in config.FIELDS:
        value = values.get(field.name, None)
        if value:
            keys.append(field.data_column)
            results.append(value)
            placeholders.append(field.placeholder)

    query = "INSERT INTO %s (%s) VALUES (%s)" % (table_name,
                                                 ",".join(keys),
                                                 ",".join(placeholders))
    cursor.execute(query, results)

def index_cb(sender, instance, signal, *args, **kwargs):
    """
    Callback for Django signal
    """
    index(instance)

from django.db.models.signals import post_save
for klass in typemap.all_classes():
    post_save.connect(index_cb, sender=klass)
