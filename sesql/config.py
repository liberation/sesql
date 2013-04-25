# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010-2013

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
"""Load the configuration of sesql from a path string"""
CONFIG_PATH_FALLBACK = 'sesql_config'

try:
    from django.conf import settings
    CONFIG_PATH = getattr(settings, 'SESQL_CONFIG_PATH', CONFIG_PATH_FALLBACK)
except ImportError:
    CONFIG_PATH = CONFIG_PATH_FALLBACK

try:  # Import form 'foo.bar'
    dot = CONFIG_PATH.rindex('.')
    submodule_name = CONFIG_PATH[dot + 1:]
    config_module = getattr(__import__(CONFIG_PATH, globals(), locals(), [], -1),
                            submodule_name)
except ValueError:  # Import form 'foo_bar'
    config_module = __import__(CONFIG_PATH, globals(), locals(), [], -1)

for param_name, param_value in config_module.__dict__.items():
    if param_name.isupper():
        globals()[param_name] = param_value
globals()['orm'] = config_module.__dict__['orm']
