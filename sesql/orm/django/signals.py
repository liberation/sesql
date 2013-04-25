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
from django.conf import settings
from django.db.models import signals

if 'sesql' in settings.INSTALLED_APPS:
    def sync_db(verbosity = 0, interactive = False, signal = None, **kwargs):
        if hasattr(signal, "_sesql_syncdb_done"):
            return
        signal._sesql_syncdb_done = True
        from sesql.datamodel import sync_db
        return sync_db(verbosity)
    signals.post_syncdb.connect(sync_db)

    def handle_index(instance, isunindex = False):
        # Trick to defer import
        from sesql import config
        from sesql.index import unindex, index, schedule_reindex
        if getattr(config, 'ASYNCHRONOUS_INDEXING', False):
            return schedule_reindex(instance)
        else:
            if isunindex:
                return unindex(instance)
            else:
                return index(instance)

    def index_cb(sender, instance, *args, **kwargs):
        handle_index(instance)
    signals.post_save.connect(index_cb)
    signals.m2m_changed.connect(index_cb)

    def unindex_cb(sender, instance, *args, **kwargs):
        handle_index(instance, True)
    signals.pre_delete.connect(unindex_cb)
