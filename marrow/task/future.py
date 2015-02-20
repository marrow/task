# encoding: utf-8

""""""

from concurrent.futures._base import PENDING, RUNNING, CANCELLED, FINISHED, Error, CancelledError, TimeoutError, Future
from marrow.package.canonical import name
from marrow.package.loader import load

from .model import Task
from .message import TaskMessage, TaskFinished, TaskCancelled


log = __import__('logging').getLogger(__name__)


def _execute_callback(callable, task):
	pass


class TaskFuture(Future):
	""""""
	
	def __init__(self, task):
		self._task = getattr(task, '_id', task)
	
	@property
	def task(self):
		return Task.objects(id=self._task)
	
	@property
	def messages(self):
		return TaskMessage.objects(task=self._task)
	
	def _invoke_callbacks(self, only=None):
		if only:
			Task.submit(_execute_callback, only, self._task)
			return
		
		for callback in self.task.scalar('callback'):
			Task.submit(_execute_callback, callback, self._task)
	
	def __repr__(self):
		return repr(self.task.get())
	
	def cancel(self):
		"""Cancel the future if possible."""
		return Task.cancel(self._task)
	
	def cancelled(self):
		return Task.cancelled(self._task)
	
	def running(self):
		return Task.running(self._task)
	
	def done(self):
		return Task.done(self._task)
	
	def add_done_callback(self, fn):
		Task.on(self._task, 'done')
		callback = name(fn)
		self.task.update(add_to_set__callback=callback)
		
		if self.done():
			Task.submit(execute_callback, callback, self._task)
	
	def result(self, timeout=None):
		return Task.result(self._task, timeout)
