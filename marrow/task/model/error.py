# encoding: utf-8

from __future__ import unicode_literals

from inspect import isclass, ismethod, isgeneratorfunction
from pytz import utc
from datetime import datetime
from mongoengine import EmbeddedDocument
from mongoengine import ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField, DateTimeField

from ..compat import py2, unicode


log = __import__('logging').getLogger(__name__)


class TaskError(EmbeddedDocument):
	meta = dict(allow_inheritance=False)
	
	frame = DynamicField(db_field='f')
	line = IntField(db_field='l')
	next = EmbeddedDocumentField('TaskError', db_field='n')
	
	def reraise(self):
		pass
