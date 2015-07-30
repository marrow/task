# encoding: utf-8

import os
import pickle
import logging
import logging.config
from functools import partial
from copy import deepcopy
from datetime import datetime
from multiprocessing import Queue
from multiprocessing.managers import BaseManager, DictProxy, Value, ValueProxy, AcquirerProxy
from threading import RLock

try:
	import Queue as queue_module
except ImportError:
	import queue as queue_module

from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from yaml import load

try:
	from yaml import CLoader as Loader
except ImportError:
	from yaml import Loader

from mongoengine import connect
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from marrow.task.message import (TaskAdded, Message, StopRunner, IterationRequest, TaskMessage,
								 TaskScheduled, ReschedulePeriodic, TaskAddedRescheduled)
from marrow.task.compat import str, unicode, iterkeys, iteritems


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


class CustomManager(BaseManager):
	pass

_tasks_data = {}

_manager = CustomManager()
_manager.register('dict', dict, DictProxy)
_manager.register('Queue', Queue)
_manager.register('RLock', RLock, AcquirerProxy)


_scheduler = BackgroundScheduler(timezone=utc)
_scheduler.start()


class RunnersStorage(object):
	def __init__(self):
		self._runners = {}#_manager.dict()

	def __getitem__(self, item):
		return self._runners[item]

	def __setitem__(self, key, value):
		data = self._runners
		data[key] = value
		self._runners = data

	def __delitem__(self, key):
		data = self._runners
		del data[key]
		self._runners = data

	def get(self, key, default_value=None):
		return self._runners.get(key, default_value)

	def get_data(self):
		return self._runners

_manager.register('RunnersStorage', RunnersStorage, exposed=['__getitem__', '__setitem__', '__delitem__', 'get', 'get_data'])


def _run_periodic_task(task_id):
	from marrow.task.model import Task

	task = Task.objects.get(id=task_id)
	task.signal(TaskAddedRescheduled)

	if not task.time.frequency:
		return

	if task.time.until:
		if datetime.now().replace(tzinfo=utc) >= task.time.until.replace(tzinfo=utc):
			return

	task.signal(ReschedulePeriodic, when=datetime.now().replace(tzinfo=utc))


class RunStatus(object):
	def __init__(self, kind, data=None):
		self.kind = kind
		self.data = data

	def __repr__(self):
		if self.data is None:
			return self.kind
		return '%s: [%s]' % (self.kind, self.data)


SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
EXCEPTION = 'EXCEPTION'
RUNNER_EXCEPTION = 'RUNNER_EXCEPTION'


class RunningTask(object):
	def __init__(self, task_id):
		self.task_id = task_id

	def handle_task(self):
		from marrow.task import task as task_decorator

		task = self.get_task()

		if not task.set_running_or_notify_cancel():
			return RunStatus(FAILURE)

		func = task.callable
		if not hasattr(func, 'context'):
			func = task_decorator(func)

		context = self.get_context(task)
		for key, value in iteritems(context):
			setattr(func.context, key, value)

		result = None
		try:
			result = task.handle()
		except Exception as exc:
			import sys, traceback
			from marrow.task.exc import TimeoutError

			exc_type, exc_val, tb = sys.exc_info()
			tb = ''.join(traceback.format_tb(tb))
			exc = (exc_type, exc_val, tb)

			if task.exception is None:
				return RunStatus(RUNNER_EXCEPTION, exc)
			return RunStatus(EXCEPTION, exc)

		return RunStatus(SUCCESS, result)

	def handle(self):
		del _runners[self.task_id]
		return self.handle_task()

	def get_context(self, task):
		return dict(
			id = task.id,
		)

	def get_task(self):
		from marrow.task.model import Task
		return Task.objects.get(id=self.task_id)


class RunningRescheduled(RunningTask):
	def handle(self):
		from marrow.task.model import Task
		Task.objects(id=self.task_id).update(set__time__completed=None)
		return super(RunningRescheduled, self).handle()


class RunningGenerator(RunningTask):
	def handle(self):
		generator = self.handle_task().data
		task = self.get_task()
		for event in IterationRequest.objects(task=self.task_id).tail():
			try:
				next(generator)
			except StopIteration:
				del _runners[self.task_id]
				break
			else:
				task._invoke_callbacks()


_manager.register('Value', Value, ValueProxy)
_manager.start()

_runners = _manager.RunnersStorage()

def _process_task(task_id, message_queue=None):
	runner = pickle.loads(_runners[task_id])
	result = runner.handle()

	if message_queue is None:
		return

	if result.kind in (EXCEPTION, RUNNER_EXCEPTION):
		exc_type, exc_val, tb = result.data
		message_queue.put((exc_type, exc_val, tb, task_id, result.kind))

	else:
		message_queue.put((result.kind, task_id))


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
		self.errors = _manager.Queue()

	@staticmethod
	def _get_config(config):
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
		self.executor.shutdown(wait)
		self.queryset.interrupt()

	def schedule_task(self, task, message):
		from apscheduler.triggers.date import DateTrigger

		trigger = DateTrigger(run_date=task.time.scheduled)
		task_id = unicode(task.id)
		_scheduler.add_job(_run_periodic_task, trigger=trigger, id=task_id, args=[task_id])

		self.logger.info('%s scheduled at %s' % (task_id, trigger))

	def add_task(self, task, message):
		if task.acquire() is None:
			self.logger.warning('Failed to acquire lock on task: %r', task)
			return

		self.logger.info('Acquired lock on task: %r', task)
		if isinstance(message, TaskAddedRescheduled):
			runner_class = RunningRescheduled
		else:
			if task.generator:
				runner_class = RunningGenerator
			else:
				runner_class = RunningTask

		task_id = unicode(task.id)
		runner = runner_class(task_id)
		# Use explicit pickling because of Python 3
		_runners[task_id] = pickle.dumps(runner)
		try:
			self.executor.submit(partial(_process_task, task_id, self.errors))
		except RuntimeError:
			return False

	def reschedule_periodic(self, task, message):
		from marrow.task.model import Task
		date_time = message.when.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
		Task.objects(id=task.id).update(set__time__scheduled=date_time, set__time__acquired=None, set__owner=None)
		task.signal(TaskScheduled, when=date_time)

	def run(self):
		for event in self.queryset.tail(timeout=self.timeout):
			while True:
				try:
					data = self.errors.get(False)
				except queue_module.Empty:
					break

				if len(data) == 2:
					if data[0] == SUCCESS:
						self.logger.info('Completed: %s', data[1])
					elif data[1] == FAILURE:
						self.logger.info('Failed: %s', data[1])
					continue

				exc_type, exc_val, tb, task_id, exc_kind = data
				error_msg = '%s(%s) in %stask %s:\n%s' % (
					exc_type.__name__, exc_val,
					'in runner at ' if exc_kind == RUNNER_EXCEPTION else '',
					task_id, tb)
				self.logger.exception(error_msg)

			if event.processed:
				continue
			event.process()

			msg = 'Process %s' % event.__class__.__name__
			if isinstance(event, TaskMessage):
				msg += ' for %s' % event.task.id
			self.logger.info(msg)

			if isinstance(event, StopRunner):
				self.logger.info('Runner is stopped')
				event.process()
				return

			if not isinstance(event, TaskMessage):
				continue

			task = event.task

			handler = {
				TaskScheduled: self.schedule_task,
				TaskAdded: self.add_task,
				TaskAddedRescheduled: self.add_task,
				ReschedulePeriodic: self.reschedule_periodic
			}.get(event.__class__, lambda task, event: None)

			if handler(task, event) is False:
				break

		self.shutdown(False)
