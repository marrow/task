# encoding: utf-8

"""Decorators."""

from inspect import isgeneratorfunction
from wrapt import decorator
from datetime import datetime, timedelta
from pytz import utc
import threading

from mongoengine import Document

from marrow.package.canonical import name
from marrow.package.loader import load

from .model import Task
from marrow.task.mock import MockTask
from .message import TaskAdded, TaskScheduled
from .compat import str, unicode


def _absolute_time(dt):
	if dt is None:
		return dt
	
	if isinstance(dt, datetime):
		return dt
	
	return datetime.utcnow().replace(tzinfo=utc) + dt


def _decorate_task(defer=False, generator=False, scheduled=False, repeating=False, wait=False):
	@decorator
	def _decorate_task_inner(wrapped, instance, args, kwargs):
		if not defer:
			return MockTask(wrapped, args, kwargs)

		task = Task(callable=name(wrapped))
		task.generator = generator
		if generator and wait:
			task.options['wait_for_iteration'] = True

		# Allow calling bound methods of Document sub-classes.
		if isinstance(instance, Document):
			# TODO: Check that this instance has a pk and has been saved!
			exc = ValueError('Model instance must be a saved')
			if instance.pk is None:
				raise exc
			cls = instance.__class__
			try:
				cls.objects.get(**{cls._fields[cls._meta['id_field']].name: instance.pk})
			except Exception:
				raise exc
			# task.reference = {'cls': instance._get_collection_name(), 'idx': unicode(instance.id)}
			task.reference = instance

		# If handling a fn.every() call...
		if repeating:
			td = args[0]
			if not isinstance(td, timedelta):
				try:
					td = timedelta(seconds=td)
				except TypeError:
					raise TypeError("Must supply timedelta frequency or number of seconds as first argument.")
			try:
				task.time.frequency = task.time.EPOCH + td
			except IndexError:
				raise TypeError("Must supply timedelta frequency or number of seconds as first argument.")
			args = args[1:]
			
			task.time.scheduled = _absolute_time(kwargs.pop('starts', None)) or datetime.now().replace(tzinfo=utc)
			task.time.scheduled += td
			task.time.until = _absolute_time(kwargs.pop('ends', None))
		
		# Or a fn.at() call...
		elif scheduled:
			try:
				task.time.scheduled = _absolute_time(args[0])
			except IndexError:
				raise TypeError("Must supply a datetime or timedelta schedule as first argument.")
			args = args[1:]

		task.args = args
		task.kwargs = kwargs
		task.save()

		if task.time.scheduled:
			task.signal(TaskScheduled, when=task.time.scheduled)
		else:
			task.signal(TaskAdded)

		return task
	
	return _decorate_task_inner


def task(_fn=None, defer=False, wait=False):
	"""Decorate a function or method for Task-based execution.
	
	By default calling the function will return a mock Task which will lazily execute the target callable the first
	time its result is requested, in the thread that requested it.  You can change this to executing remotely by
	default by calling the decorator with a truthy value passed via the `defer` keyword argument.  Regardless of this
	setting, direct and remote calls can be accessed by `function.call` and `function.defer` respectively.

	`wait` used in generator tasks. If `False` (by default) then all iterations will be executed by runner as fast
	as possible. If `True` then next iteration will be called only when requested by client.
	"""
	
	def decorate_task(fn):
		generator = isgeneratorfunction(fn)

		if not hasattr(fn, 'context'):
			fn.__dict__['context'] = threading.local()
			fn.context.id = None
		
		fn.__dict__['call'] = immediate = _decorate_task(False, generator, wait=wait)(fn)
		fn.__dict__['defer'] = deferred = _decorate_task(True, generator, wait=wait)(fn)
		fn.__dict__['at'] = _decorate_task(True, generator, scheduled=True, wait=wait)(fn)
		fn.__dict__['every'] = _decorate_task(True, generator, repeating=True, wait=wait)(fn)

		return deferred if defer else immediate
	
	if _fn:
		if isinstance(_fn, (str, unicode)):
			_fn = load(_fn)
		return decorate_task(_fn)
	
	return decorate_task
