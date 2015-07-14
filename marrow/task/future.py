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
	
	__slots__ = ('_task', )
	
	def __init__(self, task):
		self._task = getattr(task, 'pk', task)
	
	@property
	def task(self):
		return Task.objects(id=self._task)
	
	@property
	def messages(self):
		return TaskMessage.objects(task=self._task)
	
	def _invoke_callbacks(self):
		_task = self.task
		for callback in _task.scalar('callbacks'):
			from marrow.task import task
			task(callback).defer(_task)
	
	def __repr__(self):
		return repr(self.task.get())
	
	def cancel(self):
		"""Cancel the future if possible."""
		return Task.cancel(self._task)
	
	def cancelled(self):
		return bool(Task.objects.cancelled(id=self._task))
	
	def running(self):
		return bool(Task.objects.running(id=self._task))
	
	def done(self):
		return bool(Task.objects.finished(id=self._task))
	
	def add_done_callback(self, fn):
		self.task.add_callback(fn)
	
	def result(self, timeout=None):
		return self.task.wait(timeout).result
