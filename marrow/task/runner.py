# encoding: utf-8


import logging
from yaml import load
try:
	from yaml import CLoader as Loader
except ImportError:
	from yaml import Loader
from mongoengine import connect
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from marrow.task.message import TaskAdded, TaskAcquired, TaskComplete
from marrow.task.exc import AcquireFailed


logging.basicConfig()


class Runner(object):
	def __init__(self, config=None):
		self._connection = None

		config = self._get_config(config)

		ex_cls = dict(
			thread = ThreadPoolExecutor,
			process = ProcessPoolExecutor,
		)[config['type']]
		self.executor = ex_cls(max_workers=config['workers'])

		self.timeout = config['timeout']

		self._connect(config['database'])

		self.logger = logging.getLogger('%s:%s' % (__name__, config['loggername']))
		self.logger.setLevel(getattr(logging, config['loglevel'].upper(), logging.WARNING))

	def _get_config(self, config):
		base = dict(
			type = 'thread',
			workers = 3,
			database = 'marrowtask',
			timeout = None,
			loggername = self.__class__.__name__,
			loglevel = 'warning',
		)

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

	def _connect(self, db):
		if self._connection:
			return
		self._connection = connect(db)

	def _process_task(self, task):
		self.logger.info('Process task %r', task)

		import threading
		callable = task.get_callable()
		if not hasattr(callable, 'context'):
			callable.context = threading.local()
		callable.context.id = task.id

		task.handle()
		task.signal(TaskComplete, success=task.exception is None, result=task.exception or task.result)
		self.logger.info('Complete task %r', task)

	def run(self):
		for event in TaskAdded.objects.tail(timeout=self.timeout):
			task = event.task
			self.logger.info('Preprocess task %r', task)

			try:
				task.acquire()
			except AcquireFailed:
				self.logger.info('Can\'t acquire %r' % task)
				continue

			self.logger.info('Acquire task %r', task)
			from functools import partial
			self.executor.submit(partial(self._process_task, task))