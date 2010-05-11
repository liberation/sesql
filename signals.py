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


from django.db.models import signals

def sync_db(*args, **kwargs):
    # Trick to defer import
    from sesql.datamodel import sync_db
    return sync_db(*args, **kwargs)
signals.post_syncdb.connect(sync_db)

def index_cb(sender, instance, *args, **kwargs):
    # Trick to defer import
    from sesql.index import index
    return index(instance)
signals.post_save.connect(index_cb)
