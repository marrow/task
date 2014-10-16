# encoding: utf-8

from __future__ import unicode_literals

from mongoengine import Document, ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField

from ..compat import py2, unicode
from .query import CappedQuerySet
from .embed import Owner


class Message(Document):
	"""The basic properties of a MongoDB message queue."""
	
	meta = dict(
			collection = 'TaskQueue',
			max_documents = 1000,
			max_size = 10 * 1000 * 1000,
			queryset_class = CappedQuerySet,
			allow_inheritance = True,
			# abstract = True  # While technically abstract, we'd still like to query the base class.
		)
	
	sender = EmbeddedDocumentField(Owner, db_field='s', default=Owner.identity)
	
	@property
	def created(self):
		return self._id.generation_time
	
	def __repr__(self, inner=None):
		if inner:
			return '{0.__class__.__name__}({0.id}, host={1.host}, pid={1.pid}, ppid={1.ppid}, {2})'.format(self, self.sender, inner)
		
		return '{0.__class__.__name__}({0.id}, host={1.host}, pid={1.pid}, ppid={1.ppid})'.format(self, self.sender)
	
	def __str__(self):
		return "{0.__class__.__name__}".format(self)
	
	def __bytes__(self):
		return unicode(self).encode('unicode_escape')
	
	if py2:
		__unicode__ = __str__
		__str__ = __bytes__


class Keepalive(Message):
	"""An empty message used as a keepalive.
	
	Due to a quirk in MongoDB, a capped collection must have at least one record before you can 'tail' it.
	"""
	
	pass


class TaskMessage(Message):
	"""Messages which relate to queued jobs.
	
	For an easy way to monitor all messages relating to a Task, use the Task's `messages` attribute.
	"""
	
	task = ReferenceField('Task', required=True, db_field='t')
	
	def __repr__(self, inner=None):
		if inner:
			return super(TaskMessage, self).__repr__('task={0.task.id}, {1}'.format(self, inner))
		
		return super(TaskMessage, self).__repr__('task={0.task.id}'.format(self))
	
	if py2:  # pragma: no cover
		__unicode__ = __str__


class TaskAdded(TaskMessage):
	"""A new task has been added to the queue."""
	
	pass  # No additional data is required.


class TaskProgress(TaskMessage):
	"""A record broadcast back out to indicate progress on a task.
	
	While the latest `current` and `total` values are mirrored on the Task record, all messages are recorded there.
	"""
	
	current = IntField(db_field='a')
	total = IntField(db_field='b')
	
	message = StringField(db_field='mm', default=None)
	replacements = DictField(db_field='mr', default=None)
	
	@property
	def percentage(self):
		return self.current * 1.0 / self.total
	
	def __unicode__(self):
		if self.message:
			return self.message.format(**self.replacements)
		
		if self.total:
			return "{0:.0%}%".format(self.percentage)
		
		return "Task indicates progress."
	
	def __repr__(self, inner=None):
		pct = "{0:.0%}%".format(self.percentage) if self.total else "N/A"
		msg = '"{0}"'.format(self.message.format(**self.replacements)) if self.message else "None"
		return super(TaskProgress, self).__repr__('{0.current}/{0.total}, {1}, message={2}'.format(self, pct, msg))
	
	if py2:  # pragma: no cover
		__unicode__ = __str__


class TaskAcquired(TaskMessage):
	"""Indicate that a task has been acquired by a worker."""
	
	def __unicode__(self):
		return "Task {0.task.id} locked by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py2:  # pragma: no cover
		__unicode__ = __str__


class TaskCancelled(TaskMessage):
	"""Indicate that a task has been cancelled."""
	
	def __unicode__(self):
		return "Task {0.task.id} cancelled by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py2:  # pragma: no-cover
		__unicode__ = __str__


class TaskRetry(TaskMessage):
	"""Indicates the given task has been rescheduled."""
	
	def __unicode__(self):
		return "Task {0.task.id} scheduled for retry by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py2:  # pragma: no cover
		__unicode__ = __str__


class TaskComplete(TaskMessage):
	"""Indicate completion of a task.
	
	You can monitor for completion without caring about the actual result.
	"""
	
	success = BooleanField(db_field='s', default=True)
	result = DynamicField(db_field='r')
	
	def __unicode__(self):
		if self.success:
			return "Task {0.task.id} completed successfully.".format(self)
		
		return "Task {0.task.id} failed to complete successfully.".format(self)
	
	def __repr__(self, inner=None):
		return super(TaskComplete, self).__repr__('success={0!r}'.format(self.success))
	
	if py2:  # pragma: no cover
		__unicode__ = __str__
