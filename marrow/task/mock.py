# encoding: utf-8
import sys

from .compat import unicode, py2
from .model import FAILED, COMPLETE, CANCELLED, ACQUIRED, RUNNING, PENDING


class MockTask(object):
	def __init__(self, fn, args, kwargs):
		self.fn = fn
		self.args = args
		self.kwargs = kwargs
		self._result = None
		self._exception = None
		self._state = PENDING

	@property
	def result(self):
		if not self.done:
			self.handle()
		if self._exception is not None:
			raise self._exception
		return self._result

	def cancel(self):
		if self.done:
			return False
		self._state = CANCELLED
		return True

	def set_exception(self, exception):
		self._exception = exception

		typ, val, tb = sys.exc_info()
		self._exception._traceback = tb

	def set_result(self, result):
		self._result = result

	def handle(self):
		self._state = RUNNING
		try:
			self._result = self.fn(*self.args, **self.kwargs)
		except Exception as exc:
			self.set_exception(exc)
			self._state = FAILED
		else:
			self._state = COMPLETE

	def set_running_or_notify_cancel(self):
		if self.cancelled:
			return False
		self._state = ACQUIRED
		return True

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

	def __iter__(self):
		return iter(self.result)

	@property
	def state(self):
		return self._state

	@property
	def waiting(self):
		return self._state == PENDING

	@property
	def cancelled(self):
		"""Return True if the task has been cancelled."""
		return self._state == CANCELLED

	@property
	def running(self):
		"""Return True if the task is currently executing."""
		return self._state == RUNNING

	@property
	def done(self):
		"""Return True if the task was cancelled or finished executing."""
		return self._state in (CANCELLED, COMPLETE)

	@property
	def successful(self):
		return self._state == COMPLETE and self._exception is None

	@property
	def failed(self):
		return self._state == FAILED

	@property
	def acquired(self):
		return self._state == ACQUIRED
