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

#
# Full text search configuration - we must define that before the imports,
# because those are used by the imports
#

# Name of the PostgreSQL Text Search Configuration
TS_CONFIG_NAME = "simple_french"

# Name of the stopwards file, must be plain ASCII
STOPWORDS_FILE = "ascii_french"

# Global charset to use
CHARSET = "utf-8"

from fields import *
from sources import *
from django.db.models import Q
from libe import models, constants

#
# Fields and tables configuration
#

NOT_DELETED = ~Q(workflow_state = constants.WORKFLOW_STATE.DELETED)

# Configuration of SeSQL search fields
FIELDS = (ClassField("classname"),
          IntField("id"),          
          DateTimeField('created_at'),
          DateTimeField("modified_at"),
          IntField('created_by', "created_by.id"),
          IntField("modified_by", "modified_by.id"),
          IntField('workflow_state'),
          DateField('publication_date'),
          FullTextField('suptitle'),
          FullTextField("title"),
          FullTextField('subtitle',
                        [ 'subtitle', 'call_subtitle' ]),
          FullTextField("fulltext",
                        [ 'title', 'subtitle',
                          'suptitle', 'authorsInformations', 'keywords',
                          'call_title', 'call_subtitle',
                          'first_name', 'last_name',
                          'caption', 'original_caption',
                          'description',
                          'citation', 'answer',
                          'content', 'work_infos',
                          'pollchoice_set.content',
                          SubField("authors", [ "first_name", "last_name" ],
                                   condition = NOT_DELETED)
                          ],
                        primary = True,
                        ),
          FullTextField("fulltext_authors",
                        [ 'authorsInformations',
                          SubField("authors", [ "first_name", "last_name",
                                                "default_title" ],
                                   condition = NOT_DELETED)
                          ]
                        ),
          IntArrayField("authors", SubField("authors", "id",
                                            condition = NOT_DELETED)),
          IntArrayField('folders', SubField("folders", "id",
                                            condition = NOT_DELETED)),
          IntArrayField('sections', SubField("sections", "id",
                                             condition = NOT_DELETED)),
          FullTextField('keywords'),
          FullTextField('work_infos'),
          IntField('publication_number'),
          IntField('page_number'),
          StrField('source', size = 25),
          IntField('paper_kind'),
          IntField('paper_channel'),
          StrField('thema', size = 100),
          StrField('serie'),
          StrField('event', size = 100),
          IntField('typology'),
          IntField('is_reviewed'),
          IntField('is_archived'),
          DateTimeField('indexed_at', sql_default = 'NOW()')
          )

# Name of the global lookup table that should contain no real data
MASTER_TABLE_NAME = "sesql_index"

# Type map, associating Django classes to SeSQL tables
TYPE_MAP = ((models.Photo, "sesql_photo"),
            (models.Comment, "sesql_comment"),
            (models.Author, "sesql_author"),
            (models.WhoSaid, "sesql_whosaid"),
            (models.Blog, "sesql_blog"),
            (models.PaperPage, "sesql_page"),
            (models.Program, "sesql_program"),
            (models.BaseModel, "sesql_default"))

# Additional indexes to create
CROSS_INDEXES = (("classname", "modified_at"),
                 ("classname", "publication_date"),
                 ("classname", "created_at"),
                 ("classname", "publication_date", "page_number"),
                 ("publication_date", "page_number"))

# General condition to skip indexing content
SKIP_CONDITION = lambda vals: vals['workflow_state'] == constants.WORKFLOW_STATE.DELETED


#
# Query configuration
#

# Default sort order for queries 
DEFAULT_ORDER = ("publication_date",)

# Default LIMIT in short queries
DEFAULT_LIMIT = 50

# First we ask for the SMART_QUERY_INITIAL first sorted items
SMART_QUERY_INITIAL = 2500
# Then, if we have at least SMART_QUERY_THRESOLD of our limit, we go on
SMART_QUERY_THRESOLD = 0.35
# If we have a second query, we do * (wanted/result) * SMART_QUERY_RATIO 
SMART_QUERY_RATIO = 3.0

