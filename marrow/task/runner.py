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

from marrow.task.message import TaskAdded
from marrow.task.exc import AcquireFailed


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
		logger_name = next(config['logging'].get('loggers', {self.__class__.__name__: None}).iterkeys())
		self.logger = logging.getLogger(logger_name)

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
		self.logger.info('Process task %r', task)

		func = task.get_callable()
		context = self.get_context(task)
		for key, value in context.iteritems():
			setattr(func.context, key, value)

		task.handle()
		self.logger.info('Complete task %r', task)

	def run(self):
		for event in TaskAdded.objects.tail(timeout=self.timeout):
			task = event.task

			try:
				task.acquire()
			except AcquireFailed:
				self.logger.info('Can\'t acquire %r' % task)
				continue

			self.logger.info('Acquire task %r', task)
			self.executor.submit(partial(self._process_task, task))
