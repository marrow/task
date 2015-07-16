# encoding: utf-8

from __future__ import unicode_literals

from logging import getLogger
from inspect import isclass, ismethod, isgeneratorfunction
from pytz import utc
from datetime import datetime
from bson import ObjectId
from mongoengine import Document, ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField, DateTimeField
from marrow.package.canonical import name
from marrow.package.loader import load

from .compat import py2, unicode
from .exc import AcquireFailed
from .queryset import TaskQuerySet
from .structure import Owner, Retry, Progress, Times
from .message import TaskMessage, TaskAcquired, TaskAdded, TaskCancelled


log = getLogger(__name__)  # General messages.

log_acq = getLogger('task.acquire')  # Task acquisition notices.
log_rel = getLogger('task.release')  # Task release notices.
log_exc = getLogger('task.execute')  # Task execution notices.
log_ = getLogger('task.')  #
log_ = getLogger('task.')  #



class TaskPrivateMethods(object):
	"""Methods used internally to provide Task functionality.
	
	Split into a separate object (mixin) to aide in testing and to group functionality.
	"""
	
	def acquire(self):
		"""Attempt to acquire a lock on this task.
		
		Returns the reloaded object if the lock was acquired, None otherwise.
		"""
		
		# Capture the current process identity and current time.
		task = unicode(self.id)
		identity = Owner.identity()
		now = datetime.utcnow().replace(tzinfo=utc)
		
		# Attempt to acquire the task lock.
		count = self.__class__.objects(id=task, owner=None, time__acquired=None).update(
				set__owner = identity,
				set__time__acquired = now
			)
		
		# If we failed, let the caller know.
		if not count:

			log_acq.error(task)
			return None
		
		self.signal(TaskAcquired, owner=identity)  # Notify the queue.

		log_acq.info(task)
		
		return self.reload()
	
	def release(self, force=False):
		"""Attempt to release the lock on this task.
		
		This is only done if the worker is shutting down and needs to re-add the task to the pool or on pool startup
		to "un-stick" tasks that had not begun execution yet.  (In the latter case tasks in later states are nuked
		or retried.)
		
		This will use the current process identity unless "force" is true, in which case the lock will be released
		regardless of if we are the active owner of the lock or not.
		
		Returns the reloaded object if the lock was released, None otherwise.
		"""
		
		task = unicode(self.id)
		identity = self.owner if force else Owner.identity()
		
		# Attempt to release the lock.
		count = self.__class__.objects(id=task, owner=identity).update(
				set__owner = None,
				set__time__acquired = None
			)
		
		if not count:
			log.error("Failed to free lock on task: %r", self)
			log_rel.error(task)
			return None
		
		log.info("Freed lock on task: %r", self)
		log_rel.info(task)
		
		return self.reload()
	
	def _execute_standard(self, fn):
		"""Execute a standard function or method."""
		try:
			result = fn(*self.args, **self.kwargs)
		except Exception as e:
			pass
	
	def _execute_iterable(self):
		pass
	
	def execute(self):
		"""Attempt to execute this task."""
		
		ident = unicode(self.id)
		log.info("Attempting to execute task: %r", self)
		
		now = datetime.utcnow().replace(tzinfo=utc)
		
		self = self.acquire()
		if not self: return
		
		if __debug__:  # Expensive call, so we allow it to be optimized (-O) away.
			log_exc.info("%s(%s%s%s)",
					self.callable,
					', '.join(repr(i) for i in self.args),
					', ' if self.args and self.kwargs else '',
					', '.join((k + '=' + repr(v)) for k, v in self.kwargs.items())
				)
		else:
			log_exc.info(self.callable)
		
		count = self.__class__.objects(
				id=self.id,
				owner=self.owner,
				time__executed=None,
				time__cancelled=None
			).update(
				set__time__executed = now,
			)
		
		try:
			self._execute()
		except Exception as e:
			log.exception()
			pass
		
		try:
			fn = load(self.callable)  # TODO: Allow use of entry points.
		except ImportError:
			log.critical("Failed to resolve callable: %s", self._callable)
			return
		
		
		
		try:
			result = self._execute_standard()
		except:
			self.objects(id=self.id, time__executed=None).update(
					
				)
		
		count = self.objects(id=self.id, )
		
		pass


class TaskExecutorMethods(object):
	pass


class TaskFutureMethods(object):
	pass
















