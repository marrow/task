# encoding: utf-8

from mongoengine.fields import BaseField
from marrow.package.canonical import name
from marrow.package.loader import load


class PythonReferenceField(BaseField):
	"""A field that transforms a callable into a string reference using marrow.package on assignment, then back to the
	callable when accessing."""

	def to_python(self, value):
		if callable(value):
			return value
		return load(value)

	def to_mongo(self, value):
		return name(value)

	def validate(self, value, clean=True):
		if not callable(value):
			self.error('Only callables may be used in a %s' % self.__class__.__name__)

	def prepare_query_value(self, op, value):
		if not callable(value):
			return value
		return name(value)