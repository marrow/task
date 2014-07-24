# encoding: utf-8

from __future__ import unicode_literals

from os import getpid, getppid
from socket import gethostname, gethostbyname

from mongoengine import EmbeddedDocument, StringField, IntField, ListField, DynamicField

from ..compat import py2, unicode


class Owner(EmbeddedDocument):
	"""The host/process currently working on a task."""
	
	meta = dict(allow_inheritance=False)
	
	host = StringField(db_field='h')
	ppid = IntField(db_field='P')
	pid = IntField(db_field='p')
	
	@classmethod
	def identity(cls):
		return cls(host=gethostbyname(gethostname()), ppid=getppid(), pid=getpid())
	
	# Magic Python Methods
	
	def __repr__(self):
		return b'Owner("{0.host}", pid={0.pid}, ppid={0.ppid})'.format(self)
	
	def __str__(self):
		return "{0.host}:{0.ppid}.{0.pid}".format(self)
	
	def __bytes__(self):
		return unicode(self).encode('unicode_escape')
	
	if py2:
		__unicode__ = __str__
		__str__ = __bytes__


class Retry(EmbeddedDocument):
	"""The current retry and maximum number of allowable retries for a task.
	
	Defined when enqueuing or using a decorator.  The retry delay can be overridden when explicitly retrying.
	"""
	
	meta = dict(allow_inheritance=False)
	
	current = IntField(db_field='c', default=0)
	maximum = IntField(db_field='m', default=0)
	
	delay = IntField(db_field='d', default=0)  # In seconds.


class Progress(EmbeddedDocument):
	"""Task progress information.
	
	Not all tasks benefit from this; they generally need to be coded as generators yielding ProgressMessage instances.
	"""
	
	meta = dict(allow_inheritance=False)
	
	current = IntField(db_field='c', default=0)
	maximum = IntField(db_field='m', default=0)
	messages = ListField(DynamicField(), default=list)
