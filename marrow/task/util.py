# encoding: utf-8

from inspect import getmodule
from itertools import chain

try:
	from pkg_resources import iter_entry_points
except ImportError:
	iter_entry_points = None



def resolve(obj):
	"""This helper function attempts to resolve the dot-colon import path for the given object.
	
	This is the inverse of 'lookup', which will turn the dot-colon path back into the object.
	
	Python 3.3 added a substantially improved way of determining the fully qualified name for objects;
	this updated method will be used if available.  Note that running earlier versions will prevent correct
	association of nested objects (i.e. objects not at the top level of a module).
	"""
	
	if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
		obj = obj.__class__
	
	q = getattr(obj, '__qualname__', None)
	
	if not q:
		q = obj.__name__
		
		if hasattr(obj, 'im_class'):
			q = obj.im_class.__name__ + '.' + q
	
	return getmodule(obj).__name__ + ':' + q


def load(target, namespace=None):
	"""This helper function loads an object identified by a dotted-notation string.
	
	For example::
	
		# Load class Foo from example.objects
		load('example.objects:Foo')
	
	If a plugin namespace is provided simple name references are allowed.  For example::
	
		# Load the plugin named 'routing' from the 'web.dispatch' namespace
		load('routing', 'web.dispatch')
	
	Providing a namespace does not prevent full object lookup (dot-colon notation) from working.
	"""
	if namespace and ':' not in target:
		if not iter_entry_points:
			raise ImportError("Unable to import pkg_resources; do you have setuptools installed?")
		
		allowable = dict((i.name,  i) for i in iter_entry_points(namespace))
		if target not in allowable:
			raise ValueError('Unknown plugin "' + target + '"; found: ' + ', '.join(allowable))
		return allowable[target].load()
	
	parts, target = target.split(':') if ':' in target else (target, None)
	obj = __import__(parts)
	
	for part in chain(parts.split('.')[1:], target.split('.') if target else []):
		obj = getattr(obj, part)
	
	return obj
