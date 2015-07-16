# encoding: utf-8

import sys

py2 = sys.version_info < (3, )
py3 = sys.version_info > (3, )

if py3:  # pragma: no cover
	import builtins

	unicode = str
	str = bytes
	range = builtins.range
	zip = builtins.zip
	iterkeys = lambda d: iter(d.keys())
	itervalues = lambda d: iter(d.values())
	iteritems = lambda d: iter(d.items())
else:  # pragma: no cover
	import itertools

	unicode = unicode
	str = str
	range = xrange
	zip = itertools.izip
	iterkeys = lambda d: d.iterkeys()
	itervalues = lambda d: d.itervalues()
	iteritems = lambda d: d.iteritems()

try:  # pragma: no cover
	from collections import OrderedDict as odict
except ImportError:  # pragma: no cover
	from ordereddict import OrderedDict as odict
