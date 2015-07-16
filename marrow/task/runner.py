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

from marrow.task.message import TaskAdded, Message, StopRunner
from marrow.task.exc import AcquireFailed
from marrow.task.compat import str, unicode, iterkeys, iteritems


# logging.basicConfig()


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

	def get_context(self, task):
		return dict(
			id = task.id,
		)

	def _process_task(self, task):
		from marrow.task import task as task_decorator

		func = task.callable
		if not hasattr(func, 'context'):
			func = task_decorator(func)

		context = self.get_context(task)
		for key, value in iteritems(context):
			setattr(func.context, key, value)

		try:
			task.handle()
		except Exception:
			self.logger.warning('Failed: %r', task)
		else:
			self.logger.info('Completed: %r', task)

	def interrupt(self):
		self.queryset.interrupt()

	def run(self):
		for event in self.queryset.tail(timeout=self.timeout):
			if isinstance(event, StopRunner) and not event.processed:
				print("STOPPED")
				event.process()
				return
			if not isinstance(event, TaskAdded):
				continue

			task = event.task

			if task.acquire() is None:
			# except AcquireFailed:
				self.logger.warning("Failed to acquire lock on task: %r", task)
				continue

			self.logger.info("Acquired lock on task: %r", task)
			self.executor.submit(partial(self._process_task, task))
