# encoding: utf-8

from __future__ import unicode_literals

from datetime import datetime

from mongoengine import Document, ReferenceField, IntField, EmbeddedDocumentField, BooleanField, DynamicField, DateTimeField

from .compat import py2, py3, unicode
from .queryset import CappedQuerySet
from .structure import Owner


class Message(Document):
	"""The basic properties of a MongoDB message queue."""
	
	meta = dict(
			collection = 'TaskQueue',
			max_documents = 65535,
			max_size = 100 * 1024 * 1024,
			queryset_class = CappedQuerySet,
			allow_inheritance = True,
			# abstract = True  # While technically abstract, we'd still like to query the base class.
		)
	
	sender = EmbeddedDocumentField(Owner, db_field='s', default=Owner.identity)
	created = DateTimeField(db_field='c', default=datetime.utcnow)
	processed = BooleanField(db_field='p', default=False)

	def __repr__(self, inner=None):
		if inner:
			return '{0.__class__.__name__}({0.id}, host={1.host}, pid={1.pid}, ppid={1.ppid}, {2})'.format(self, self.sender, inner)
		
		return '{0.__class__.__name__}({0.id}, host={1.host}, pid={1.pid}, ppid={1.ppid})'.format(self, self.sender)
	
	def __str__(self):
		return "{0.__class__.__name__}".format(self)
	
	def __bytes__(self):
		return unicode(self).encode('unicode_escape')

	if py2:  # pragma: no cover
		__unicode__ = __str__
		__str__ = __bytes__

	# def process(self):
	# 	Message.objects(id=self.id).update(set___processed=True)
	#
	# @property
	# def processed(self):
	# 	return Message.objects.scalar('_processed').get(id=self.id)


class Keepalive(Message):
	"""An empty message used as a keepalive.
	
	Due to a quirk in MongoDB, a capped collection must have at least one record before you can 'tail' it.
	"""
	
	pass


class StopRunner(Message):
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
	
	def __unicode__(self):
		return "{0.__class__.__name__}".format(self)
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class TaskAdded(TaskMessage):
	"""A new task has been added to the queue."""
	
	pass  # No additional data is required.


class TaskAddedRescheduled(TaskAdded):
	pass


class TaskScheduled(TaskAdded):
	"""A scheduled task has been added to the queue."""
	
	when = DateTimeField(db_field='w')

	def __repr__(self, inner=None):
		return super(TaskScheduled, self).__repr__('when={0}'.format(self.when))


class TaskProgress(TaskMessage):
	"""A record broadcast back out to indicate progress on a task.
	
	While the latest `current` and `total` values are mirrored on the Task record, all messages are recorded there.
	"""

	NORMAL, FINISHED, FAILED = range(3)

	current = IntField(db_field='a')
	total = IntField(db_field='b')

	result = DynamicField(db_field='r')
	status = IntField(db_field='st', default=NORMAL, choices=[
		(NORMAL, 'normal'),
		(FINISHED, 'finished'),
		(FAILED, 'failed'),
	])

	@property
	def percentage(self):
		return self.current * 1.0 / self.total
	
	def __repr__(self, inner=None):
		pct = "{0:.0%}".format(self.percentage) if self.total else "N/A"
		msg = unicode(self.result)
		return super(TaskProgress, self).__repr__('{0.current}/{0.total}, {1}, result={2}, status={3}'.format(self, pct, msg, self.get_status_display()))
	
	def __unicode__(self):
		if self.result:
			return unicode(self.result)
		
		if self.total:
			return "{0:.0%}".format(self.percentage)
		
		return "Task indicates progress."
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class TaskAcquired(TaskMessage):
	"""Indicate that a task has been acquired by a worker."""
	owner = EmbeddedDocumentField(Owner, db_field='o')
	
	def __unicode__(self):
		return "Task {0.task.id} locked by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class ReschedulePeriodic(TaskMessage):
	when = DateTimeField(db_field='w')

	def __repr__(self, inner=None):
		return super(ReschedulePeriodic, self).__repr__('when={0}'.format(self.when))


class TaskRetry(TaskMessage):
	"""Indicates the given task has been rescheduled."""
	
	def __unicode__(self):
		return "Task {0.task.id} scheduled for retry by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class TaskFinished(TaskMessage):
	"""Common parent class for cancellation or completion."""
	pass


class TaskCancelled(TaskFinished):
	"""Indicate that a task has been cancelled."""
	
	def __unicode__(self):
		return "Task {0.task.id} cancelled by PID {0.sender.pid} on host: {0.sender.host}".format(self)
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class TaskCompletedPeriodic(TaskFinished):
	pass


class TaskComplete(TaskFinished):
	"""Indicate completion of a task.
	
	You can monitor for completion without caring about the actual result.
	"""
	
	success = BooleanField(db_field='su', default=True)
	result = DynamicField(db_field='r')
	
	def __repr__(self, inner=None):
		return super(TaskComplete, self).__repr__('success={0!r}'.format(self.success))
	
	def __unicode__(self):
		if self.success:
			return "Task {0.task.id} completed successfully.".format(self)
		
		return "Task {0.task.id} failed to complete successfully.".format(self)
	
	if py3:  # pragma: no cover
		__str__ = __unicode__


class IterationRequest(TaskMessage):
	pass
