# encoding: utf-8

from __future__ import unicode_literals

from logging import getLogger
from inspect import isclass, ismethod, isgeneratorfunction
from pytz import utc
from datetime import datetime
from mongoengine import Document, ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField, DateTimeField

from ..compat import py2, unicode
from ..exc import AcquireFailed
from ..util import resolve, load
from .query import TaskQuerySet
from .embed import Owner, Retry, Progress
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
		count = self.__class__.objects(id=task, _owner=None, _acquired=None).update(
				set___owner = identity,
				set___acquired = now
			)
		
		# If we failed, let the caller know.
		if not count:
			log.warning("Failed to acquire lock on task: %r", self)
			log_acq.error(task)
			return None
		
		TaskAcquired(self, identity).save()  # Notify the queue.
		
		log.info("Acquired lock on task: %r", self)
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
		identity = self.identity if force else Owner.identity()
		
		# Attempt to release the lock.
		count = self.__class__.objects(id=task, _owner=identity).update(
				set___owner = None,
				set___acquired = None
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
			result = fn(*self._args, **self._kwargs)
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
					self._callable,
					', '.join(repr(i) for i in self._args),
					', ' if self._args and self._kwargs else '',
					', '.join((k + '=' + repr(v)) for k, v in self._kwargs.items())
				)
		else:
			log_exc.info(self._callable)
		
		count = self.objects(id=self.id, owner=self.owner, executed=None, cancelled=None).update(
				set__executed = now,
			)
		
		try:
			self._execute()
		except Exception as e:
			log.exception()
			pass
		
		
		try:
			fn = load(self._callable)  # TODO: Allow use of entry points.
		except ImportError:
			log.critical("Failed to resolve callable: %s", self._callable)
			return
		
		
		
		try:
			result = self._execute_standard()
		except:
			self.objects(id=self.id, executed=None).update(
					
				)
		
		count = self.objects(id=self.id, )
		
		pass


class TaskExecutorMethods(object):
	pass


class TaskFutureMethods(object):
	pass



class Task(Document, TaskPrivateMethods, TaskExecutorMethods, TaskFutureMethods):
	"""The definition of a remotely executed task."""
	
	meta = dict(
			collection = 'Tasks',
			queryset_class = TaskQuerySet,
			allow_inheritance = False,
			indexes = [
				]
		)
	
	# These are used to store the relevant private task data in MongoDB.
	
	_callable = StringField(db_field='fn', required=True)  # Should actually be a PythonReferenceField.
	_args = ListField(DynamicField(), db_field='ap', default=list)
	_kwargs = DictField(db_field='ak', default=dict)
	
	_exception = DynamicField(db_field='exc', default=None)
	_result = DynamicField(db_field='res', default=None)
	_callback = ListField(StringField(), db_field='rcb')  # Technically these are just the callables for new tasks to enqueue!
	
	_when = DateTimeField(db_field='ds', default=None)
	_acquired = DateTimeField(db_field='da', default=None)
	_executed = DateTimeField(db_field='de', default=None)
	_completed = DateTimeField(db_field='dc', default=None)
	_cancelled = DateTimeField(db_field='dx', default=None)
	_expires = DateTimeField(db_field='dp', default=None)  # After completion we don't want these records sticking around.
	
	_creator = EmbeddedDocumentField(Owner, db_field='c', default=Owner.identity)
	_owner = EmbeddedDocumentField(Owner, db_field='o')
	_retry = EmbeddedDocumentField(Retry, db_field='r', default=Retry)
	_progress = EmbeddedDocumentField(Progress, db_field='p', default=Progress)
	
	# Python Magic Methods
	
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
	
	# Marrow Task API
	
	@property
	def messages(self):
		"""Return a tailable query matching all TaskMessage objects for this task."""
		
		return TaskMessage.objects(task=self)
	
	@property
	def state(self):
		"""Return a token indicating the current state of the task."""
		
		if self._cancelled:
			return 'cancelled'
		
		if self._completed:
			if self._exception:
				return 'failed'
			
			return 'complete'
		
		if self._executed:
			return 'running'
		
		if self._acquired:
			return 'acquired'
		
		return 'pending'
	
	@property
	def created(self):
		return self._id.generation_time if self._id else None
	
	@property
	def performance(self):
		"""Determine various timed durations involved with this task.
		
		The values of the dictionary represent "in" times, i.e. "acquired in X", "executed in X", etc.
		
		If a value is None then the job hasn't entered a state where measurements for that value can be provided.
		
		These values represent how long it took for the state change to occur, not from the inital scheduling time.
		
		Note, also, that these values aren't "live"; `.reload()` the record beforehand if needed.
		"""
		
		if not self._acquired:
			return dict(acquired=None, executed=None, completed=None)
		
		data = dict(acquired=self._acquired - self._created)
		
		data['executed'] = (self._acquired - self._executed) if self._executed else None
		
		# We don't distinguish between cancellation, errors, and successes here.
		data['completed'] = (self._executed - self._completed) if self._completed else None
		
		return data
	
	
	
	

	
	# Futures-compatible pseudo-internal API.
	
	def set_running_or_notify_cancel(self):
		pass
	
	def set_result(self, result):
		pass
	
	def set_exception(self, exception):
		typ, value, tb = sys.exc_info()
		tb = tb.tb_next
		tb = ''.join(traceback.format_exception(typ, value, tb))
	
	# Futures-compatible executor API.
	
	def _notify_added(self, task):
		try:
			TaskAdded(task=task).save()
		except:
			log.exception("Unable to submit task.")
			task.cancel()
			raise Exception("Unable to submit task.")
		
		return task
	
	@classmethod
	def submit(cls, fn, *args, **kwargs):
		"""Submit a task for execution as soon as possible."""
		
		log.debug("submitting new task: {0!r}", fn)
		
		record = cls(callable=fn, args=args, kwargs=kwargs).save()  # Step one, create and save a pending task record.
		record._notify_added()  # Step two, notify everyone waiting for tasks.
		
		# Because the record acts like a Futures object, we can just return it.
		return record
	
	@classmethod
	def at(cls, dt, fn, *args, **kw):
		"""Submit a task for execution at a specific date/time."""
		
		log.debug("submitting task for execution at {0}: {1!r}", dt.isoformat(), fn)
		
		record = cls(callable=fn, args=args, kwargs=kw, when=dt).save()  # As above, but with a date.
		record._notify_added()  # Step two, notify everyone waiting for tasks.
		
		return record
	
	@classmethod
	def map(cls, fn, *iterables, **kw):
		"""Return an iterator yielding the results of the parallel map call.
		
		Raises TimeoutError if __next__ is called and the result isn't available within timeout seconds.
		"""
		timeout = kw.pop('timeout', None)
		
		if kw: raise TypeError("unexpected keyword argument: {0}".format(', '.join(kw)))
		
		yield
		
		return
	
	@classmethod
	def shutdown(cls, wait=True):
		"""Signal the worker pool that it should stop processing after currently excuting tasks have completed."""
		pass
	
	# Futures-compatible future API.
	
	def cancel(self):
		"""Cancel the task if possible.
		
		Returns True if the task was cancelled, False otherwise.
		"""
		
		log.debug("Attempting to cancel: %r", self)
		
		# Atomically attempt to mark the task as cancelled if it hasn't been executed yet.
		# If the task has already been run (completed or not) it's too late to cancel it.
		# Interesting side-effect: multiple calls will cancel multiple times, updating the time of cencellation.
		if not Task.objects(id=self.id, executed=None).update(set__cancelled=datetime.utcnow().replace(tzinfo=utc)):
			return False
		
		try:
			TaskCancelled(task=self).save()
		except:
			log.exception("Unable to notify task cancellation: %s", str(self.id))
			return False
		
		log.info("task %s cancelled", str(self.id))
		
		return True
	
	def cancelled(self):
		"""Return Ture if the task has been cancelled."""
		return bool(Task.objects(id=self.id).cancelled().count())
	
	def running(self):
		"""Return True if the task is currently executing."""
		return bool(Task.objects(id=self.id).running().count())
	
	def done(self):
		"""Return True if the task was cancelled or finished executing."""
		return bool(Task.objects(id=self.id).filter(completed__ne=None).count())
	
	def result(self, timeout=None):
		"""Return the value returned by the call.
		
		If the task has not completed yet, wait up to `timeout` seconds, or forever if `timeout` is None.
		
		On timeout, TimeoutError is raised.  If the task is cancelled, CancelledError is raised.
		
		If the task failed (raised an exception internally) than TaskError is raised.
		"""
		
		completed, result, exception = Task.objects(id=self.id).scalar('completed', '_result', '_exception').first()
		
		if completed:  # We can exit early, this task was previously completed.
			if exception: raise Exception(exception)
			return result
		
		# Otherwise we wait.
		for event in self.messages.tail(timeout=timeout):
			pass
		
		pass
	
	def exception(self, timeout=None):
		pass
	
	def add_done_callback(self, fn):
		Task.objects(id=self.id).update()
	
	# Futures extension for support of generators.
	def __iter__(self):
		return self
	
	def next(self, timeout=None):
		pass  # TODO
	
	__next__ = next
