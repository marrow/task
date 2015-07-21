# encoding: utf-8


import logging
import logging.config
from functools import partial
from copy import deepcopy

from yaml import load
try:
	from yaml import CLoader as Loader
except ImportError:
	from yaml import Loader
from mongoengine import connect
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from marrow.task.message import TaskAdded, Message, StopRunner, IterationRequest, TaskMessage
from marrow.task.exc import AcquireFailed
from marrow.task.compat import str, unicode, iterkeys, iteritems


# logging.basicConfig()


_runners = {}


DEFAULT_CONFIG = dict(
	runner = dict(
		timeout = None,
		use = 'thread',
		max_workers = 3,
	),
	database = dict(
		host = 'mongodb://localhost/marrowTask',
	),
	logging = dict(
		version = 1,
	)
)


class RunningTask(object):
	def __init__(self, runner, task):
		self.runner = runner
		self.task = task

	def handle(self):
		print('RUNNER RUNNING')
		from marrow.task import task as task_decorator

		if not self.task.set_running_or_notify_cancel():
			return

		func = self.task.callable
		if not hasattr(func, 'context'):
			func = task_decorator(func)

		context = self.get_context(self.task)
		for key, value in iteritems(context):
			setattr(func.context, key, value)

		result = None
		try:
			result = self.task.handle()
		except Exception:
			self.runner.logger.warning('Failed: %r', self.task)
		else:
			self.runner.logger.info('Completed: %r', self.task)

		# import ipdb; ipdb.set_trace()
		# TODO: Not sure that it should be made that way.
		from marrow.task.model import GeneratorTaskIterator
		if isinstance(result, GeneratorTaskIterator):
			_runners[unicode(self.task.id)] = RunningGenerator(result)
			return

		del _runners[unicode(self.task.id)]

	def get_context(self, task):
		return dict(
			id = task.id,
		)


class RunningGenerator(object):
	def __init__(self, iterator):
		self.iterator = iterator

	def next(self):
		try:
			next(self.iterator)
		except StopIteration:
			del _runners[unicode(self.iterator.task.id)]
		else:
			self.iterator.task._invoke_callbacks()


def _process_task(task_id):
	_runners[task_id].handle()


class Runner(object):
	def __init__(self, config=None):
		config = self._get_config(config)

		ex_cls = dict(
			thread = ThreadPoolExecutor,
			process = ProcessPoolExecutor,
		)[config['runner'].pop('use')]

		self.timeout = config['runner'].pop('timeout')
		self.executor = ex_cls(**config['runner'])

		self._connection = None
		self._connect(config['database'])

		logging.config.dictConfig(config['logging'])
		# Get first logger name from config or class name.
		logger_name = next(iterkeys(config['logging'].get('loggers', {self.__class__.__name__: None})))
		self.logger = logging.getLogger(logger_name)
		self.queryset = Message.objects

	def _get_config(self, config):
		base = deepcopy(DEFAULT_CONFIG)

		if config is None:
			return base

		if isinstance(config, dict):
			base.update(config)
			return base

		opened = False
		if isinstance(config, (str, unicode)):
			try:
				config = open(config, 'rt')
			except Exception:
				return base
			else:
				opened = True

		data = load(config, Loader=Loader)
		if opened:
			config.close()

		base.update(data)
		return base

	def _connect(self, config):
		if self._connection:
			return
		self._connection = connect(**config)


	def shutdown(self, wait=None):
		if not wait:
			# Shutdown after currently processing task.
			self.queryset.interrupt()
			self.logger.info('Runner is interrupted')
			return

		if wait is True:
			# Shutdown after all tasks that added before this call.
			StopRunner.objects.create()
			self.logger.info('Runner finishing work')
			return

		# Wait until no new messages added for `wait` seconds.
		import time

		last = Message.objects.count()
		end_time = time.time() + wait

		self.logger.info('Runner waiting for end of messages')
		while time.time() < end_time:
			count = Message.objects.count()
			if count > last:
				last = count
				end_time = time.time() + wait

		self.queryset.interrupt()

	def run(self):
		for event in self.queryset.tail(timeout=self.timeout):
			if event.processed:
				continue
			event.process()

			print(event.__class__.__name__)

			if isinstance(event, StopRunner):
				self.logger.info("Runner is stopped")
				event.process()
				return

			if not isinstance(event, TaskMessage):
				continue

			task = event.task

			if isinstance(event, TaskAdded):
				if task.acquire() is None:
					self.logger.warning("Failed to acquire lock on task: %r", task)
					continue

				self.logger.info("Acquired lock on task: %r", task)
				r = RunningTask(self, task)
				_runners[unicode(task.id)] = r
				self.executor.submit(partial(_process_task, unicode(task.id)))

			elif isinstance(event, IterationRequest):
				runner = _runners.get(unicode(task.id))
				if runner is not None:
					runner.next()
