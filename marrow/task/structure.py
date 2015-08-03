# encoding: utf-8

from __future__ import unicode_literals

from os import getpid, getppid
from socket import gethostname, gethostbyname
from datetime import datetime
from pytz import utc

from mongoengine import EmbeddedDocument, EmbeddedDocumentField, StringField, IntField, ListField, DictField, DynamicField, DateTimeField

from .compat import py2, unicode, iteritems


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
		return 'Owner("{0.host}", pid={0.pid}, ppid={0.ppid})'.format(self)
	
	def __str__(self):
		return "{0.host}:{0.ppid}:{0.pid}".format(self)
	
	def __bytes__(self):
		return unicode(self).encode('unicode_escape')
	
	if py2:  # pragma: no cover
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
	
	# Magic Python Methods
	
	def __repr__(self):
		if not self.maximum:
			return 'Retry(None)'
		
		return 'Retry({0.current}/{0.maximum}, {0.delay})'.format(self)
	
	def __str__(self):
		if not self.maximum:
			return "First and only attempt."
		
		return "Attempt {0.current} of {0.maximum}, {0.delay} seconds between tries.".format(self)
	
	def __bytes__(self):
		return unicode(self).encode('unicode_escape')
	
	if py2:  # pragma: no cover
		__unicode__ = __str__
		__str__ = __bytes__


class Progress(EmbeddedDocument):
	"""Task progress information.
	
	Not all tasks benefit from this; they generally need to be coded as generators yielding Progress instances.
	"""
	
	meta = dict(allow_inheritance=False)
	
	current = IntField(db_field='c', default=0)
	maximum = IntField(db_field='m', default=0)
	messages = ListField(DynamicField(), default=list)
	replacements = DictField(db_field='r', default=dict)
	
	@property
	def percentage(self):
		if not self.maximum: return None
		return self.current * 1.0 / self.maximum
	
	def __unicode__(self):
		if self.messages:
			return self.messages[-1].format(**dict(self.replacements, progress=self))
		
		if self.maximum:
			return "{0.percentage:.0%}% ({0.current}/{0.total})".format(self)
		
		return "Task indicates progress."
	
	def __repr__(self, inner=None):
		pct = "{0:.0%}%".format(self.percentage) if self.maximum else "N/A"
		return 'Progress({0.current}/{0.maximum}, {1}, messages={2})'.format(self, pct, len(self.messages))


class Error(EmbeddedDocument):
	meta = dict(allow_inheritance=False)
	
	frame = DynamicField(db_field='f')
	line = IntField(db_field='l')
	next = EmbeddedDocumentField('self', db_field='n')
	
	def reraise(self):
		pass


class Times(EmbeddedDocument):
	meta = dict(allow_inheritance=False)
	
	EPOCH = datetime(2000, 1, 1, 0, 0, 0, tzinfo=utc)  # We store TimeDeltas as an offset from this.
	
	created = DateTimeField(db_field='i', default=lambda: datetime.utcnow().replace(tzinfo=utc))
	scheduled = DateTimeField(db_field='s', default=None)
	frequency = DateTimeField(db_field='f', default=None)  # As offset from EPOCH.
	until = DateTimeField(db_field='u', default=None)  # For use with repeating tasks.
	acquired = DateTimeField(db_field='a', default=None)
	executed = DateTimeField(db_field='e', default=None)
	completed = DateTimeField(db_field='c', default=None)
	cancelled = DateTimeField(db_field='x', default=None)
	expires = DateTimeField(db_field='p', default=None)  # After completion we don't want these records sticking around.

	def __repr__(self):
		fdt = lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')
		text = ', '.join('{0}={1}'.format(key, fdt(value)) for key, value in iteritems(self._data) if value is not None)
		return 'Times({0})'.format(text)
