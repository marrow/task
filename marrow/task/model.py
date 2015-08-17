# encoding: utf-8

from __future__ import unicode_literals

import pickle
from logging import getLogger
from inspect import isclass, ismethod, isgeneratorfunction, isgenerator
from pytz import utc
from datetime import datetime
from concurrent.futures import CancelledError
from bson import ObjectId
from mongoengine import Document, ReferenceField, IntField, StringField, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField, DateTimeField, GenericReferenceField
from wrapt.wrappers import FunctionWrapper
from marrow.package.canonical import name
from marrow.package.loader import load

from .compat import py2, py33, unicode, range, zip
from .exc import AcquireFailed, TimeoutError
from .queryset import TaskQuerySet
from .structure import Owner, Retry, Progress, Times
from .message import (TaskMessage, TaskAcquired, TaskAdded, TaskCancelled, TaskComplete, TaskFinished,
					  StopRunner, TaskCompletedPeriodic, ReschedulePeriodic, TaskProgress)
from .methods import TaskPrivateMethods
from .field import PythonReferenceField


log = getLogger(__name__)  # General messages.

log_acq = getLogger('task.acquire')  # Task acquisition notices.
log_rel = getLogger('task.release')  # Task release notices.
log_exc = getLogger('task.execute')  # Task execution notices.
log_ = getLogger('task.')  #


def encode(obj):
	result = pickle.dumps(obj)
	if py2:
		result = result.encode('ascii')
	return result


def decode(string):
	if py2:
		string = string.decode('ascii')
	return pickle.loads(string)


def utcnow():
	return datetime.utcnow().replace(tzinfo=utc)


class GeneratorTaskIterator(object):
	def __init__(self, task, gen):
		self.task = task
		self.generator = gen
		self.iteration = 0
		self.total = None

	def __iter__(self):
		return self

	def process_iteration_result(self, result, status):
		self.iteration += 1

		if isinstance(result, TaskProgress):
			if result.id is None:
				result.save()
			return

		if not (isinstance(result, (tuple, list)) and len(result) >= 2):
			self.task.signal(TaskProgress, current=self.iteration, total=self.total, result=result, status=status)
			return

		self.iteration, self.total = result
		data = {
			'current': self.iteration,
			'total': self.total
		}
		if len(result) > 2:
			data['result'] = result[2]

		self.task.signal(TaskProgress, status=status, **data)

	def next(self):
		try:
			item = next(self.generator)

		except StopIteration as exception:
			value = getattr(exception, 'value', None)
			self.process_iteration_result(value, TaskProgress.FINISHED)
			self.task._complete_task()
			raise

		except Exception as exception:
			exc = self.task.set_exception(exception)
			self.process_iteration_result(exc, TaskProgress.FAILED)
			self.task._complete_task()
			raise StopIteration

		else:
			Task.objects(id=self.task.id).update(push__task_result=item)
			self.process_iteration_result(item, TaskProgress.NORMAL)
			return item

	__next__ = next


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
	callbacks = ListField(PythonReferenceField(), db_field='rcb')  # Technically these are just the callables for new tasks to enqueue!
	
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
		if self.done:
			for item in self.result:
				yield item
			raise StopIteration

		from marrow.task.message import IterationRequest

		self.signal(IterationRequest)
		for entry in TaskProgress.objects(task=self).tail():
			if entry.status == TaskProgress.FINISHED:
				if entry.result is not None:
					yield entry.result

				if py33:
					raise StopIteration(entry.result)
				else:
					raise StopIteration

			elif entry.status == TaskProgress.FAILED:
				raise self.exception

			if entry.result is not None:
				yield entry.result

			self.signal(IterationRequest)

	def result_iterator(self):
		"""Iterate the results of a generator task.

		It is an error condition to attempt to iterate a non-generator task.
		"""
		if not self.generator:
			raise ValueError('Cannot use on non-generator tasks.')
		return self._result_iterator()

	def __str__(self):
		"""Get the unicode string result of this task."""
		return unicode(self.result)
	
	def __bytes__(self):
		"""Get the byte string result of this task."""
		return unicode(self).encode('unicode_escape')
	
	def __int__(self):
		return int(self.result)
	
	def __float__(self):
		return float(self.result)
	
	if py2:  # pragma: no cover
		__unicode__ = __str__
		__str__ = __bytes__
	
	def next(self, timeout=None):
		pass  # TODO
	
	__next__ = next

	def get_messages(self, kind=TaskMessage):
		return kind.objects(task=self)

	# Marrow Task API
	
	@property
	def messages(self):
		"""Return a tailable query matching all TaskMessage objects for this task."""
		
		return self.get_messages()
	
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
		if not self.done:
			self.wait()

		result, exception, freq = Task.objects.scalar('task_result', 'task_exception', 'time__frequency').get(id=self.id)
		if freq is None:
			if exception is not None:
				raise decode(exception['exception'])
			return result

		def periodic_iterator():
			stop_index = None
			for count, event in enumerate(TaskFinished.objects(task=self).tail(), 1):
				if isinstance(event, TaskCancelled):
					raise CancelledError

				if isinstance(event, TaskCompletedPeriodic):
					stop_index = ReschedulePeriodic.objects(task=self).count()

				else:
					if not event.success:
						raise decode(event.result['exception'])

					yield event.result

				if stop_index is not None and count == stop_index:
					raise StopIteration

		return periodic_iterator()

	@property
	def exception_info(self):
		exc = Task.objects.scalar('task_exception').get(id=self.id)
		if exc is None:
			return None, None
		return decode(exc['exception']), exc['traceback']

	@property
	def exception(self):
		return self.exception_info[0]

	def _invoke_callbacks(self):
		from marrow.task import task

		for callback in self.callbacks:
			task(callback).defer(self)

	def _complete_task(self):
		Task.objects(id=self.id).update(set__time__completed=utcnow())

		exception, result = Task.objects.scalar('task_exception', 'task_result').get(id=self.id)
		self.signal(TaskComplete, success=self.task_exception is None, result=exception or result)

		self.reload('callbacks', 'time')
		if self.successful:
			self._invoke_callbacks()

	def handle(self):
		if self.done:
			return self.task_result

		func = self.callable
		if isinstance(func, FunctionWrapper):
			func = func.call

		if self.reference:
			from functools import partial
			func = partial(func, self.reference)

		result = None

		try:
			result = func(*self.args, **self.kwargs)
		except Exception as exception:
			self.set_exception(exception)
			self._complete_task()
			raise
		else:
			if isgenerator(result):
				Task.objects(id=self.id).update(set__task_result=[])
				return GeneratorTaskIterator(self, result)
			self.set_result(result)

		self._complete_task()
		return result

	def add_done_callback(self, callback):
		from marrow.task import task

		if self.done or self.cancelled:
			task(callback).defer(self)
			return

		self.callbacks.append(callback)
		self.save()

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
		if self.cancelled:
			return False
		if self.acquired:
			Task.objects(id=self.id).update(set__time__executed=utcnow())
			return True
		raise RuntimeError('Task in unexpected state [%s: %s]' % (self.id, self.state))
	
	def set_result(self, result):
		Task.objects(id=self.id).update(set__task_result=result)
		return result
	
	def set_exception(self, exception):
		import sys, traceback

		if exception is None:
			return

		typ, value, tb = sys.exc_info()
		if value is None:
			exc = dict(
				exception = encode(exception),
				traceback = None
			)
		else:
			tb = tb.tb_next
			tb = ''.join(traceback.format_exception(typ, value, tb))
			exc = dict(
				exception = encode(value),
				traceback = tb
			)
		Task.objects(id=self.id).update(set__task_exception=exc)
		return exc
	
	# Futures-compatible executor API.
	
	def _notify_added(self):
		try:
			TaskAdded(task=self).save()
		except:
			log.exception("Unable to submit task.")
			self.cancel()
			raise Exception("Unable to submit task.")
		
		return self
	
	@classmethod
	def submit(cls, fn, *args, **kwargs):
		"""Submit a task for execution as soon as possible."""
		
		log.info("", fn)
		
		record = cls(callable=fn, args=args, kwargs=kwargs).save()  # Step one, create and save a pending task record.
		record.signal(TaskAdded)  # Step two, notify everyone waiting for tasks.
		
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

		def map_iterator():
			import time

			if timeout is not None:
				end_time = time.time() + timeout

			tasks = []
			for args in zip(*iterables):
				task = cls(callable=fn, args=args).save()
				task.signal(TaskAdded)
				tasks.append(task)

			failed = None
			for task in tasks:
				if failed is not None:
					Task.cancel(task)
					continue

				try:
					if timeout is None:
						yield task.result
					else:
						yield task.wait(timeout=end_time - time.time()).result
				except TimeoutError as exc:
					failed = exc
					Task.cancel(task)

			if failed is not None:
				raise failed

		return map_iterator()

	@classmethod
	def shutdown(cls, wait=True):
		"""Signal the worker pool that it should stop processing after currently executing tasks have completed."""
		from marrow.task.runner import Runner
		if isinstance(wait, Runner):
			wait.shutdown()
			return

		StopRunner.objects.create()

	# Helper methods.
	def signal(self, kind, **kw):
		message = kind(task=self, sender=self.creator, **kw)
		
		for i in range(3):
			try:
				message.save()
			except Exception:
				pass

	# Futures-compatible future API.
	
	@classmethod
	def cancel(cls, task):
		"""Cancel the task if possible.
		
		Returns True if the task was cancelled, False otherwise.
		"""
		
		task = ObjectId(getattr(task, 'id', task))
		
		log.debug("Attempting to cancel task {0}.".format(task), extra=dict(task=task))

		# Atomically attempt to mark the task as cancelled if it hasn't been executed yet.
		# If the task has already been run (completed or not) it's too late to cancel it.
		# Interesting side-effect: multiple calls will cancel multiple times, updating the time of cancellation.
		if Task.objects.scalar('time__frequency').get(id=task) is None:
			qkws = {'time__executed': None}
		else:
			qkws = {}
		if not Task.objects(id=task, **qkws).update(set__time__cancelled=utcnow()):
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
		"""Return True if the task has been cancelled."""
		return bool(Task.objects.cancelled(id=self.id).count())

	@property
	def running(self):
		"""Return True if the task is currently executing."""
		return bool(Task.objects.running(id=self.id).count())

	@property
	def done(self):
		"""Return True if the task was cancelled or finished executing."""
		return bool(Task.objects.finished(id=self.id).count())

	@property
	def successful(self):
		return bool(Task.objects.complete(id=self.id).count())

	@property
	def failed(self):
		return bool(Task.objects.failed(id=self.id).count())

	@property
	def acquired(self):
		return bool(Task.objects(id=self.id, time__acquired__ne=None).count())