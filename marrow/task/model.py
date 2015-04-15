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
from .methods import TaskPrivateMethods


log = getLogger(__name__)  # General messages.

log_acq = getLogger('task.acquire')  # Task acquisition notices.
log_rel = getLogger('task.release')  # Task release notices.
log_exc = getLogger('task.execute')  # Task execution notices.
log_ = getLogger('task.')  #
log_ = getLogger('task.')  #



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
	
	callable = StringField(db_field='fn', required=True)  # Should actually be a PythonReferenceField.
	args = ListField(DynamicField(), db_field='ap', default=list)
	kwargs = DictField(db_field='ak', default=dict)
	
	reference = GenericReferenceField(db_field='ref', default=None)
	generator = BooleanField(db_field='gen', default=False)
	
	exception = DynamicField(db_field='exc', default=None)
	result = DynamicField(db_field='res', default=None)
	callback = ListField(StringField(), db_field='rcb')  # Technically these are just the callables for new tasks to enqueue!
	
	time = EmbeddedDocumentField(Times, db_field='t', default=Times)
	creator = EmbeddedDocumentField(Owner, db_field='c', default=Owner.identity)
	owner = EmbeddedDocumentField(Owner, db_field='o')
	retry = EmbeddedDocumentField(Retry, db_field='r', default=Retry)
	progress = EmbeddedDocumentField(Progress, db_field='p', default=Progress)
	
	# Python Magic Methods
	
	def __repr__(self, inner=None):
		if inner:
			return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid}, {2})'.format(self, self.sender, inner)
	
		return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid})'.format(self, self.sender)
	
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
		
		callable = name(fn)
		
		log.info("", fn)
		
		record = cls(callable=fn, args=args, kwargs=kwargs).save()  # Step one, create and save a pending task record.
		record.notify(TaskAdded)  # Step two, notify everyone waiting for tasks.
		
		# Because the record acts like a Futures object, we can just return it.
		return record
	
	@classmethod
	def at(cls, dt, fn, *args, **kw):
		"""Submit a task for execution at a specific date/time."""
		
		log.debug("submitting task for execution at {0}: {1!r}", dt.isoformat(), fn)
		
		record = cls(callable=fn, args=args, kwargs=kw, scheduled=dt).save()  # As above, but with a date.
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
	
	# Helper methods.
	
	def _signal(self, kind, **kw):
		message = kind(task=self, **kw)
		
		for i in range(3):
			try:
				message.save()
			except:
				pass
	
	# Futures-compatible future API.
	
	@classmethod
	def cancel(cls, task):
		"""Cancel the task if possible.
		
		Returns True if the task was cancelled, False otherwise.
		"""
		
		task = ObjectId(getattr(task, '_id', task))
		
		log.debug("Attempting to cancel task {0}.".format(task), extra=dict(task=task))
		
		# Atomically attempt to mark the task as cancelled if it hasn't been executed yet.
		# If the task has already been run (completed or not) it's too late to cancel it.
		# Interesting side-effect: multiple calls will cancel multiple times, updating the time of cencellation.
		if not Task.objects(id=task, executed=None).update(set__cancelled=datetime.utcnow().replace(tzinfo=utc)):
			return False
		
		for i in range(3):  # We attempt three times to notify the queue.
			try:
				TaskCancelled(task=task).save()
			except:
				log.exception("Unable to broadcast cancellation of task {0}.".format(task),
						extra = dict(task=task, attempt=i + 1))
			else:
				break
		else:
			return False
		
		log.info("task {0} cancelled".format(task), extra=dict(task=task, action='cancel'))
		
		return True
	
	def cancelled(self):
		"""Return Ture if the task has been cancelled."""
		return bool(Task.objects.cancelled(id=self).count())
	
	def running(self):
		"""Return True if the task is currently executing."""
		return bool(Task.objects.running(id=self).count())
	
	def done(self):
		"""Return True if the task was cancelled or finished executing."""
		return bool(Task.objects.finished(id=self).count())
	
	def result(self, timeout=None):
		"""Return the value returned by the call.
		
		If the task has not completed yet, wait up to `timeout` seconds, or forever if `timeout` is None.
		
		On timeout, TimeoutError is raised.  If the task is cancelled, CancelledError is raised.
		
		If the task failed (raised an exception internally) than TaskError is raised.
		"""
		
		completed, result, exception = Task.objects(id=self).scalar('completed', '_result', '_exception').get()
		
		if completed:  # We can exit early, this task was previously completed.
			if exception:
				raise Exception(exception)
			
			return result
		
		# Otherwise we wait.
		for event in TaskFinished.objects(task=self._task).tail(timeout):
			if isinstance(event, TaskCancelled):
				raise CancelledError()
			
			if not message.success:
				raise Exception(message.result)
			
			return message.result
			
		else:
			raise TimeoutError()
	
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
