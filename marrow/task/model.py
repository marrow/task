# encoding: utf-8

from __future__ import unicode_literals

import inspect
import os.path
import pickle
from logging import getLogger
from inspect import isgenerator
from datetime import datetime

from pytz import utc
from concurrent.futures import CancelledError
from mongoengine import (Document, DictField, EmbeddedDocumentField, BooleanField, DynamicField, ListField,
						 GenericReferenceField, DoesNotExist)
from wrapt.wrappers import FunctionWrapper

from .compat import py2, py33, unicode, range, zip
from .exc import TimeoutError
from .queryset import TaskQuerySet
from .structure import Owner, Retry, Progress, Times
from .message import (TaskMessage, TaskAdded, TaskCancelled, TaskComplete, TaskFinished, TaskCompletedPeriodic, TaskProgress)
from .methods import TaskPrivateMethods
from .field import PythonReferenceField

log = getLogger(__name__)  # General messages.

log_acq = getLogger('task.acquire')  # Task acquisition notices.
log_rel = getLogger('task.release')  # Task release notices.
log_exc = getLogger('task.execute')  # Task execution notices.
log_ = getLogger('task.')  #


MONGOENGINE_PATH = os.path.dirname(inspect.getfile(Document))


def encode(obj):
	result = pickle.dumps(obj)
	if py2:
		result = result.encode('ascii')
	return result


def decode(string):
	if py2:
		string = str(string.decode('ascii'))
	return pickle.loads(string)


def utcnow():
	return datetime.utcnow().replace(tzinfo=utc)


class GeneratorTaskIterator(object):
	"""Iterator used by generator tasks handler.

	Store given generator and task. Iterates on generator, augment iteration result with progress metadata and store it
	in database. Update task with exception and result, call it's callbacks if necessary. Signal with task progress."""

	def __init__(self, task, gen):
		""":param task: :class:`Task` instance of currently handled task.
		:param generator: generator of given task.

		**Attributes:**

		* **iteration** -- index of current generator's iteration.
		* **total** -- total count of iterations."""

		self.task = task
		self.generator = gen
		self.iteration = 0
		self.total = None

	def __iter__(self):
		return self

	def process_iteration_result(self, result, status):
		"""Signal with task progress.

		Augment result with progress metadata, if it not provided with result itself."""

		self.iteration += 1

		if isinstance(result, TaskProgress):
			if result.id is None:
				result.save()
			return result.result

		if not (isinstance(result, (tuple, list)) and len(result) in (2, 3)):
			self.task.signal(TaskProgress, current=self.iteration, total=self.total, result=result, status=status)
			return result

		self.iteration, self.total = result[:2]
		data = {
			'current': self.iteration,
			'total': self.total,
			'result': None
		}
		if len(result) > 2:
			data['result'] = result[2]

		self.task.signal(TaskProgress, status=status, **data)
		return data['result']

	def next(self):
		"""Iterate given generator.

		Obtain next iteration's result. Signal with task progress. Update task's result and exception if necessary."""

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
			result = self.process_iteration_result(item, TaskProgress.NORMAL)
			Task.objects(id=self.task.id).update(push__task_result=result)
			self.task._invoke_callbacks(iteration=True)
			return result

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
	options = DictField(db_field='op')
	
	# Python Magic Methods

	def __repr__(self, inner=None):
		if inner:
			return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid}, {2})'.format(self, self.creator, inner)
	
		return '{0.__class__.__name__}({0.id}, {0.state}, host={1.host}, pid={1.pid}, ppid={1.ppid})'.format(self, self.creator)

	# Python Magic Methods - These will block on task completion.

	def __iter__(self):
		# __iter__ used in model's save method.
		# Get caller's file path and if it is not in mongoengine return iterator of task's result.
		caller_frame = inspect.stack()[1][0]
		if caller_frame.f_code.co_filename.startswith(MONGOENGINE_PATH):
			return super(Task, self).__iter__()

		return self.iterator()

	def _generator_iterator(self):
		if self.done:
			for item in self.result:
				yield item
			raise StopIteration

		from marrow.task.message import IterationRequest

		if self.options.get('wait_for_iteration'):
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

			if self.options.get('wait_for_iteration'):
				self.signal(IterationRequest)

	def _periodic_iterator(self):
		stop_index = None
		for current, event in enumerate(TaskFinished.objects(task=self).tail(), 1):
			if isinstance(event, (TaskCompletedPeriodic, TaskCancelled)):
				stop_index = TaskComplete.objects(task=self).count()

			else:
				if not event.success:
					raise decode(event.result['exception'])

				yield event.result

			if stop_index is not None and current >= stop_index:
				raise StopIteration

	def iterator(self):
		"""Iterate the results of a generator or periodic task.

		It is an error condition to attempt to iterate a non-generator or non-periodic task.
		"""
		if not (self.generator or self.time.frequency):
			raise ValueError('Only periodic and generator tasks are iterable.')

		if self.time.frequency:
			return self._periodic_iterator()

		return self._generator_iterator()

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
		
		data['executed'] = (self.time.executed - self.time.acquired) if self.time.executed else None
		
		# We don't distinguish between cancellation, errors, and successes here.
		data['completed'] = (self.time.completed - self.time.executed) if self.time.completed else None
		
		return data

	@property
	def result(self):
		if not self.done:
			self.wait()

		try:
			result, exception, freq = Task.objects.scalar('task_result', 'task_exception', 'time__frequency').get(id=self.id)
		except DoesNotExist:
			return None

		if freq is None:
			if exception is not None:
				raise decode(exception['exception'])
			return result

		event = TaskComplete.objects(task=self).order_by('-id').first()
		if event is None:
			if TaskCancelled.objects(task=self).count():
				raise CancelledError
			return None

		if not event.success:
			raise decode(event.result['exception'])

		return event.result

	@property
	def exception_info(self):
		try:
			exc = Task.objects.scalar('task_exception').get(id=self.id)
		except DoesNotExist:
			return None, None
		if exc is None:
			return None, None
		return decode(exc['exception']), exc['traceback']

	@property
	def exception(self):
		return self.exception_info[0]

	def _invoke_callbacks(self, iteration=False):
		from marrow.task import task

		callbacks = self.options.get('iteration_callbacks', []) if iteration else self.callbacks
		for callback in callbacks:
			task(callback).defer(self)

	def _complete_task(self):
		Task.objects(id=self.id).update(set__time__completed=utcnow())

		exception, result = Task.objects.scalar('task_exception', 'task_result').get(id=self.id)
		self.signal(TaskComplete, success=self.task_exception is None, result=exception or result)

		try:
			self.reload('callbacks', 'time')
		except DoesNotExist:
			return

		if self.successful:
			self._invoke_callbacks()

	def handle(self):
		if not self.time.frequency and self.done:
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

	def add_callback(self, callback, iteration=False):
		from marrow.task import task

		if self.done or self.cancelled:
			task(callback).defer(self)
			return

		if not iteration:
			self.callbacks.append(callback)
			self.save()
			return

		prf = PythonReferenceField()
		prf.validate(callback)
		Task.objects(id=self.id).update(push__options__iteration_callbacks=prf.to_mongo(callback))

	def wait(self, timeout=None, periodic=False):
		msgclasses = [TaskComplete, TaskCancelled]
		if self.time.scheduled is None and self.time.until is None:
			periodic = False
		if periodic:
			msgclasses.append(TaskCompletedPeriodic)
		msgclasses = [cls._class_name for cls in msgclasses]
		for event in TaskFinished.objects(task=self, __raw__={'_cls': {'$in': msgclasses}}).tail(timeout):
			if isinstance(event, TaskCancelled):
				raise CancelledError
			if periodic and not isinstance(event, TaskCompletedPeriodic):
				continue
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
		if not self.signal(TaskAdded):
			log.exception("Unable to submit task.")
			self.cancel()
			raise Exception("Unable to submit task.")
		
		return self
	
	@classmethod
	def submit(cls, fn, *args, **kwargs):
		"""Submit a task for execution as soon as possible."""
		from marrow.task.future import TaskFuture

		log.info("", fn)
		
		record = cls(callable=fn, args=args, kwargs=kwargs).save()  # Step one, create and save a pending task record.
		record._notify_added()  # Step two, notify everyone waiting for tasks.
		
		return TaskFuture(record)
	
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

	# Helper methods.
	def signal(self, kind, **kw):
		message = kind(task=self, sender=self.creator, **kw)
		
		for i in range(3):
			try:
				message.save()
			except Exception:
				pass
			else:
				return True
		return False

	# Futures-compatible future API.
	
	def cancel(self):
		"""Cancel the task if possible.
		
		Returns True if the task was cancelled, False otherwise.
		"""
		
		return bool(Task.objects.cancel(id=self.id))

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
