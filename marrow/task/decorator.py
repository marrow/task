# encoding: utf-8

"""Decorators."""

from inspect import isgeneratorfunction
from wrapt import decorator
from datetime import datetime
from pytz import utc
import threading

from mongoengine import Document

from marrow.package.canonical import name
from marrow.package.loader import load

from .model import Task
from .message import TaskAdded, TaskScheduled
from .compat import str, unicode


def _absolute_time(dt):
	if dt is None:
		return dt
	
	if isinstance(dt, datetime):
		return dt
	
	return datetime.utcnow().replace(tzinfo=utc) + dt


def _decorate_task(defer=False, generator=False, scheduled=False, repeating=False):
	@decorator
	def _decorate_task_inner(wrapped, instance, args, kwargs):
		if not defer:
			return wrapped(*args, **kwargs)

		task = Task(callable=name(wrapped))
		task.generator = generator

		# Allow calling bound methods of Document sub-classes.
		if isinstance(instance, Document):
			# TODO: Check that this instance has a pk and has been saved!
			task.reference = instance
		
		# If handling a fn.every() call...
		if repeating:
			try:
				task.time.frequency = task.time.EPOCH + args[0]
			except IndexError:
				raise TypeError("Must supply timedelta frequency as first argument.")
			args = args[1:]
			
			task.time.scheduled = _absolute_time(kwargs.pop('starts', None))
			task.time.until = _absolute_time(kwargs.pop('ends', None))
		
		# Or a fn.at() call...
		elif scheduled:
			try:
				task.time.scheduled = _absolute_time(args[0])
			except IndexError:
				raise TypeError("Must supply a datetime or timedelta schedule as first argument.")
			args = args[1:]

		task.args = args
		task.save()

		if task.time.scheduled:
			task.signal(TaskScheduled, when=task.time.scheduled)
		else:
			task.signal(TaskAdded)

		return task
	
	return _decorate_task_inner


def task(_fn=None, defer=False):
	"""Decorate a function or method for Task-based execution.
	
	By default calling the function will return a mock Task which will lazily execute the target callable the first
	time its result is requested, in the thread that requested it.  You can change this to executing remotely by
	default by calling the decorator with a truthy value passed via the `defer` keyword argument.  Regardless
	"""
	
	def decorate_task(fn):
		generator = isgeneratorfunction(fn)

		if not hasattr(fn, 'context'):
			fn.context = threading.local()
			fn.context.id = None
		
		fn.call = immediate = _decorate_task(False, generator)(fn)
		fn.defer = deferred = _decorate_task(True, generator)(fn)
		fn.at = _decorate_task(True, generator, scheduled=True)(fn)
		fn.every = _decorate_task(True, generator, repeating=True)(fn)

		return deferred if defer else immediate
	
	if _fn:
		if isinstance(_fn, (str, unicode)):
			_fn = load(_fn)
		return decorate_task(_fn)
	
	return decorate_task
