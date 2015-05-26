# encoding: utf-8

from __future__ import unicode_literals

import pickle
from logging import getLogger
from inspect import isclass, ismethod, isgeneratorfunction, isgenerator
from pytz import utc
from datetime import datetime
from collections import namedtuple
from concurrent.futures import CancelledError
from bson import ObjectId
from mongoengine import Document, ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField, DateTimeField, GenericReferenceField
from wrapt.wrappers import FunctionWrapper
from marrow.package.canonical import name
from marrow.package.loader import load

from .compat import py2, unicode
from .exc import AcquireFailed, TimeoutError
from .queryset import TaskQuerySet
from .structure import Owner, Retry, Progress, Times
from .message import TaskMessage, TaskAcquired, TaskAdded, TaskCancelled, TaskComplete, TaskIterated, TaskFinished
from .methods import TaskPrivateMethods
from .field import PythonReferenceField


log = getLogger(__name__)  # General messages.

log_acq = getLogger('task.acquire')  # Task acquisition notices.
log_rel = getLogger('task.release')  # Task release notices.
log_exc = getLogger('task.execute')  # Task execution notices.
log_ = getLogger('task.')  #


def encode(obj):
	return pickle.dumps(obj).encode('ascii')


def decode(string):
	return pickle.loads(string.decode('ascii'))


def utcnow():
	return datetime.utcnow().replace(tzinfo=utc)


class GeneratorTaskIterator(object):
	def __init__(self, task, gen):
		self.task = task
		self.generator = gen

	def __iter__(self):
		return self

	def next(self):
		try:
			item = next(self.generator)

		except StopIteration as exception:
			value = getattr(exception, 'value', None)
			self.task.signal(TaskIterated, status=TaskIterated.FINISHED, result=value)
			result = list(TaskIterated.objects(task=self.task, result__ne=None).scalar('result'))
			self.task.set_result(result)
			self.task._complete_task()
			raise

		except Exception as exception:
			exc = self.task.set_exception(exception)
			self.task.signal(TaskIterated, status=TaskIterated.FAILED, result=exc)
			self.task._complete_task()
			raise StopIteration()

		else:
			self.task.signal(TaskIterated, status=TaskIterated.NORMAL, result=item)
			return item

	__next__ = next


_ExceptionInfo = namedtuple('_ExceptionInfo', 'type exception traceback')


class Task(TaskPrivateMethods, Document):  # , TaskPrivateMethods, TaskExecutorMethods, TaskFutureMethods
	"""The definition of a remotely executed task."""
	
	meta = dict(
			collection = 'Tasks',
			queryset_class = TaskQuerySet,
			allow_inheritance = False,
			indexes = [
				]
		)
	
	# These are used to store the relevant private task data in MongoDB.
	
	callable = PythonReferenceField(db_field='fn', required=True)  # Should actually be a PythonReferenceField.
	args = ListField(DynamicField(), db_field='ap', default=list)
	kwargs = DictField(db_field='ak', default=dict)
	
	reference = GenericReferenceField(db_field='ref', default=None)
	generator = BooleanField(db_field='gen', default=False)
	
	task_exception = DynamicField(db_field='exc', default=None)
	task_result = DynamicField(db_field='res', default=None)
	callback = ListField(PythonReferenceField(), db_field='rcb')  # Technically these are just the callables for new tasks to enqueue!
	
	time = EmbeddedDocumentField(Times, db_field='t', default=Times)
	creator = EmbeddedDocumentField(Owner, db_field='c', default=Owner.identity)
	owner = EmbeddedDocumentField(Owner, db_field='o')
	retry = EmbeddedDocumentField(Retry, db_field='r', default=Retry)
	progress = EmbeddedDocumentField(Progress, db_field='p', default=Progress)
	
	# Python Magic Methods
	
	def __repr__(self, inner=None):
		if inner:
			return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid}, {2})'.format(self, self.creator, inner)
	
		return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid})'.format(self, self.creator)

	# Python Magic Methods - These will block on task completion.

	def __iter__(self):
		import inspect
		import os.path

		# __iter__ used in model's save method.
		# Get caller's file path and if it is not in mongoengine return iterator of task's result.
		caller_frame = inspect.stack()[1][0]
		mongoengine_path = os.path.dirname(inspect.getfile(Document))
		if caller_frame.f_code.co_filename.startswith(mongoengine_path):
			return super(Task, self).__iter__()

		return self.result_iterator()

	def _result_iterator(self):
		if self.time.completed:
			for item in self.result:
				yield item
			raise StopIteration

		for entry in TaskIterated.objects(task=self).tail():
			if entry.status == TaskIterated.FINISHED:
				if entry.result is not None:
					yield entry.result

				raise StopIteration(value=entry.result)

			elif entry.status == TaskIterated.FAILED:
				raise self.exception

			if entry.result is None:
				continue

			yield entry.result

	def result_iterator(self):
		"""Iterate the results of a generator task.

		It is an error condition to attempt to iterate a non-generator task.
		"""
		if not self.generator:
			raise ValueError('Cannot use on non-generator tasks.')
		return self._result_iterator()
	
	def __str__(self):
		"""Get the unicode string result of this task."""
		self.wait()  # Doesn't block if already completed.
		return unicode(self.result)
	
	def __bytes__(self):
		"""Get the byte string result of this task."""
		return unicode(self).encode('unicode_escape')
	
	def __int__(self):
		self.wait()
		return int(self.result)
	
	def __float__(self):
		self.wait()
		return float(self.result)
	
	if py2:  # pragma: no cover
		__unicode__ = __str__
		__str__ = __bytes__
	
	def next(self, timeout=None):
		pass  # TODO
	
	__next__ = next
	
	# Marrow Task API
	
	@property
	def messages(self):
		"""Return a tailable query matching all TaskMessage objects for this task."""
		
		return TaskMessage.objects(task=self)
	
	@property
	def state(self):
		"""Return a token indicating the current state of the task."""
		
		if self.time.cancelled:
			return 'cancelled'
		
		if self.time.completed:
			if self.exception:
				return 'failed'
			
			return 'complete'
		
		if self.time.executed:
			return 'running'
		
		if self.time.acquired:
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
		
		if not self.time.acquired:
			return dict(acquired=None, executed=None, completed=None)
		
		data = dict(acquired=self.time.acquired - self.time.created)
		
		data['executed'] = (self.time.acquired - self.time.executed) if self.time.executed else None
		
		# We don't distinguish between cancellation, errors, and successes here.
		data['completed'] = (self.time.executed - self.time.completed) if self.time.completed else None
		
		return data

	@property
	def result(self):
		if (self.task_result is None) and (self.task_exception is None):
			self.reload()
		return self.task_result

	@property
	def exception_info(self):
		if (self.task_exception is None) and (self.task_result is None):
			self.reload()
		exc = self.task_exception
		if exc is None:
			return _ExceptionInfo(None, None, None)
		return _ExceptionInfo(decode(exc['type']), decode(exc['exception']), exc['traceback'])

	@property
	def exception(self):
		return self.exception_info.exception


	def _complete_task(self):
		Task.objects(id=self.id).update(set__time__completed=utcnow())

		self.signal(TaskComplete, success=self.task_exception is None, result=self.task_exception or self.task_result)

		from marrow.task import task
		self.reload('callback', 'time')
		if self.successful:
			for callback in self.callback:
				task(callback).defer(self)

	def handle(self):
		if self.time.completed:
			return self.task_result

		func = self.callable
		if isinstance(func, FunctionWrapper):
			func = func.call

		result = None
		Task.objects(id=self.id).update(set__time__executed=utcnow())

		try:
			result = func(*self.args, **self.kwargs)
		except Exception as exception:
			self.set_exception(exception)
			self._complete_task()
			raise
		else:
			if isgenerator(result):
				return GeneratorTaskIterator(self, result)
			self.set_result(result)

		self._complete_task()
		return result

	def add_callback(self, callback):
		from marrow.task import task
		self.callback.append(callback)
		self.save()
		if self.completed:
			task(callback).defer(self)

	def wait(self, timeout=None):
		for event in TaskFinished.objects(task=self).tail(timeout):
			if isinstance(event, TaskCancelled):
				raise CancelledError
			break
		else:
			raise TimeoutError('%r is timed out.' % self)

		return self.reload()

	
	# Futures-compatible pseudo-internal API.
	
	def set_running_or_notify_cancel(self):
		pass
	
	def set_result(self, result):
		self.task_result = result
		self.save()
		return result
	
	def set_exception(self, exception):
		import sys, traceback

		if exception is None:
			return

		typ, value, tb = sys.exc_info()
		tb = tb.tb_next
		tb = ''.join(traceback.format_exception(typ, value, tb))
		exc = dict(
			type = encode(typ),
			exception = encode(value),
			traceback = tb
		)
		self.task_exception = exc
		self.save()
		return exc
	
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
	def signal(self, kind, **kwargs):
		return self._signal(kind, **kwargs)
	
	def _signal(self, kind, **kw):
		message = kind(task=self, sender=self.creator, **kw)
		
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
		if not Task.objects(id=task, time__executed=None).update(set__time__cancelled=utcnow()):
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

	# Properties
	@property
	def waiting(self):
		return bool(Task.objects.accepted(id=self.id).count())

	@property
	def cancelled(self):
		"""Return Ture if the task has been cancelled."""
		return bool(Task.objects.cancelled(id=self.id).count())
	
	@property
	def running(self):
		"""Return True if the task is currently executing."""
		return bool(Task.objects.running(id=self.id).count())
	
	@property
	def completed(self):
		"""Return True if the task was cancelled or finished executing."""
		return bool(Task.objects.finished(id=self.id).count())

	@property
	def successful(self):
		return bool(Task.objects.complete(id=self.id).count())

	@property
	def failed(self):
		return bool(Task.objects.failed(id=self.id).count())
	
	def result_old(self, timeout=None):
		"""Return the value returned by the call.

		If the task has not completed yet, wait up to `timeout` seconds, or forever if `timeout` is None.

		On timeout, TimeoutError is raised.  If the task is cancelled, CancelledError is raised.

		If the task failed (raised an exception internally) than TaskError is raised.
		"""

		# completed, result, exception = Task.objects(id=self).scalar('completed', 'task_result', 'task_exception').get()
		self.reload()

		if self.time.completed:  # We can exit early, this task was previously completed.
			if self.task_exception:
				raise Exception(self.task_exception)

			return self.task_result

		# Otherwise we wait.
		for event in TaskFinished.objects(task=self._task).tail(timeout):
			if isinstance(event, TaskCancelled):
				raise CancelledError()

			if not message.success:
				raise Exception(message.result)

			return message.result

		else:
			raise TimeoutError()
	
	# def exception(self, timeout=None):
	# 	pass
	
	def add_done_callback(self, fn):
		Task.objects(id=self.id).update()
