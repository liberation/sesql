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
"""
Contain the various kind of field aggregators/fetchers/...
"""


# Automatic dispatcher
def guess_source(what):
    """
    Guess what is this source

    - AbstractSource will stay as it is
    - a list or tuple will become TextAggregate
    - a name with () in it will be a MethodCaller
    - a name with . in it will be a SubField
    - everything else will be a SimpleField
    """
    if isinstance(what, AbstractSource):
        return what
    if isinstance(what, (list, tuple)):
        return TextAggregate(*what)
    if isinstance(what, dict):
        return WeightedAggregate(what)
    if not isinstance(what, str):
        raise ValueError, "what is neither AbstractSource, list, dict nor string"
    if "." in what:
        what = what.strip(".")
        return SubField(*(what.split(".", 1)))
    if what.endswith('()'):
        return MethodCaller(what[:-2])
    return SimpleField(what)

# Main classes

class AbstractSource(object):
    """
    Pure virtual class
    """
    def load_data(self, obj):
        """
        Load data from a Django object
        """
        raise NotImplementedError

    def __call__(self, obj):
        return self.load_data(obj)

class ClassSource(AbstractSource):
    """
    Get the class of the object
    """
    def __init__(self, dereference_proxy = False):
        """
        Constructor
        If dereference_proxy is set to True, proxy models will be
        considered as their base classe - this is only for Django ORM
        """
        self.dereference_proxy = dereference_proxy

    def load_data(self, obj):
        """
        Load data from a Django object
        """
        klass = obj.__class__
        if self.dereference_proxy:
            if hasattr(obj, '_meta'):
                if getattr(obj._meta, 'proxy', False):
                    klass = getattr(obj._meta, 'proxy_for_model', klass)
        return klass.__name__


class SimpleField(AbstractSource):
    """
    We index a single field
    """
    def __init__(self, name):
        """
        Constructor
        """
        self.name = name

    def load_data(self, obj):
        """
        Get the data directly
        """
        from sesql import config
        try:
            return getattr(obj, self.name, None)
        except config.orm.not_found:
            return None

class MethodCaller(SimpleField):
    """
    What we index is the result of a method
    """
    def load_data(self, obj):
        """
        Call the method, if it is callable
        """
        method = getattr(obj, self.name, None)
        if callable(method):
            return method()

# Walker
class SubField(AbstractSource):
    """
    Walk inside related objects, like .authors.id
    Return a list (that should TextAggregated  or used in a Array field)
    """
    def __init__(self, child, getter, condition = None):
        """
        Constructor
        """
        self.child = guess_source(child)
        self.getter = guess_source(getter)
        self.condition = condition

    def load_data(self, obj):
        """
        Get the data from the sub-object(s)
        """
        what = self.child.load_data(obj)

        if callable(getattr(what, "all", None)):
            # We have a "all" method ? Consider it's many ?
            try:
                if self.condition:
                    what = what.filter(self.condition)
                what = what.all()
            except Exception, e:
                what = []

            res = []
            for w in what:
                data = self.getter(w)
                if isinstance(data, (list, tuple)):
                    res.extend(list(data))
                else:
                    res.append(data)
            return res
        else:
            # One ?
            return self.getter(what)

# Aggregate

class TextAggregate(AbstractSource):
    """
    Aggregate on several text indexes
    """
    def __init__(self, *sources):
        """
        Constructor
        """
        self.sources = [ guess_source(s) for s in sources ]

    def load_data(self, obj):
        """
        Get the data directly
        """
        values = [ source.load_data(obj) for source in self.sources ]
        return self.collapse(values)

    @staticmethod
    def collapse(values):
        """
        Collapse recursively lists or tuples into string
        """
        from sesql import config

        if not values:
            return u""
        if isinstance(values, unicode):
            return values
        if not isinstance(values, (list, tuple)):
            return unicode(str(values), config.CHARSET)
        values = [ TextAggregate.collapse(v) for v in values ]
        values = [ v for v in values if v ]
        return u" ".join(values)

class WeightedAggregate(TextAggregate):
    """
    Aggregate on several text indexes, but with weight
    Input must be a dictionnary of { weight : field }
    """
    def __init__(self, sources):
        """
        Constructor
        """
        self.sources = dict([ (k, guess_source(v)) for k,v in sources.items() ])
        self.weights = self.sources.keys()

    def load_data(self, obj, weight = None):
        """
        Get the data directly but limiting to given weight if given
        """
        if not weight:
            values = [ source.load_data(obj) for source in self.sources.values() ]
            return self.collapse(values)

        source = self.sources.get(weight, None)
        if source is None:
            return u""
        return source.load_data(obj)

class FirstOf(AbstractSource):
    """
    Gets the first non-None field
    """
    def __init__(self, *sources):
        """
        Constructor
        """
        self.sources = [ guess_source(s) for s in sources ]

    def load_data(self, obj):
        """
        Get the data directly
        """
        for source in self.sources:
            data = source.load_data(obj)
            if data is not None:
                return data


