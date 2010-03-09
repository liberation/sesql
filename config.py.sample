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

from datamodel import *
from django.db.models import Q
from libe import models

FIELDS = (IntField("id"),
          StrField("classname", size = 50),
          DateField("modifiedAt"),
          FullTextField("title"),
          FullTextField("description", "getDescription()"),
          IntArrayField("authors", "authors.id"),
          FullTextField("fulltextAuthors",
                        [ 'authorsInformations',
                          SubField("authors", [ "firstname", "lastname",
                                                "defaultTitle" ],
                                   condition = ~Q(workflowState = 'deleted'))
                          ]
                        ),
          
          FullTextField("fulltext",
                        [ 'title', 'getDescription()',
                          SubField("authors", [ "firstname", "lastname" ],
                                   condition = ~Q(workflowState = 'deleted'))
                          ],
                        primary = True,
                        ),
          DateField('indexedAt', sql_default = 'NOW()'),
          )

MASTER_TABLE_NAME = "sesql_index"

TYPE_MAP = ((models.Photo, "sesql_photo"),
            (models.Comment, "sesql_comment"),
            (models.BaseModel, "sesql_default"))

