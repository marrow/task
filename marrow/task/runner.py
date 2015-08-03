# encoding: utf-8

import os
import pickle
import logging
import logging.config
from functools import partial
from copy import deepcopy
from datetime import datetime
from multiprocessing import Queue
from multiprocessing.managers import BaseManager, DictProxy, Value, ValueProxy, AcquirerProxy, ListProxy
from threading import RLock, current_thread, Thread

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
								 TaskScheduled, ReschedulePeriodic, TaskAddedRescheduled, TaskCompletedPeriodic)
from marrow.task.compat import str, unicode, iterkeys, iteritems, itervalues


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
_manager.register('Queue', Queue)


class RunnersStorage(object):
	def __init__(self):
		self._runners = {}

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
			task.signal(TaskCompletedPeriodic)
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
		# self.queryset = Message.objects

		self.message_queue = _manager.Queue()
		self.message_thread = None

	def run(self):
		def _handle_messages():
			while True:
				try:
					data = self.message_queue.get(True)
				except IOError:
					return
				# 	continue

				if data is None:
					return

				if isinstance(data, (str, unicode)):
					self.logger.info(data)
					continue

				if len(data) == 2:
					if data[0] == SUCCESS:
						self.logger.info('Completed: %s', data[1])

					elif data[1] == FAILURE:
						self.logger.warning('Failed: %s', data[1])

					continue

				exc_type, exc_val, tb, task_id, exc_kind = data
				error_msg = '%s(%s) in %stask %s:\n%s' % (
					exc_type.__name__, exc_val,
					'in runner at ' if exc_kind == RUNNER_EXCEPTION else '',
					task_id, tb)
				self.logger.exception(error_msg)

		self.message_thread = Thread(target=_handle_messages)
		self.message_thread.start()

		for i in range(self.executor._max_workers):
			self.executor.submit(run, timeout=self.timeout, message_queue=self.message_queue)

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

	def get_alive_workers_count(self):
		try:
			workers = self.executor._threads
		except AttributeError:
			workers = self.executor._processes

		if not workers:
			return 0

		return sum(worker.is_alive() for worker in (itervalues(workers) if isinstance(workers, dict) else workers))

	def shutdown(self, wait=None):
		for i in range(self.get_alive_workers_count()):
			StopRunner.objects.create()
		self.executor.shutdown(wait)
		try:
			self.message_queue.put(None)
		except Exception:
			pass
		self.message_queue.close()
		self.message_queue.join_thread()
		self.message_thread.join()
		# self.queryset.interrupt()
		self.logger.info("SHUTDOWN")


def run(timeout=None, message_queue=None):
	scheduler = BackgroundScheduler(timezone=utc)
	scheduler.start()

	def schedule_task(task, message):
		from apscheduler.triggers.date import DateTrigger

		if task.cancelled:
			return

		trigger = DateTrigger(run_date=task.time.scheduled)
		task_id = unicode(task.id)
		scheduler.add_job(_run_periodic_task, trigger=trigger, id=task_id, args=[task_id])

		# message_queue.put('%s scheduled at %s' % (task_id, trigger))

	def add_task(task, message):
		if task.acquire() is None:
			# message_queue.put('Failed to acquire lock on task: %r' % task)
			return

		# message_queue.put('Acquired lock on task: %r' % task)
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
			_process_task(task_id, message_queue)
		except RuntimeError:
			return False

	def reschedule_periodic(task, message):
		from marrow.task.model import Task
		date_time = message.when.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
		if not Task.objects(id=task.id, time__cancelled=None).update(set__time__scheduled=date_time, set__time__acquired=None, set__owner=None):
			return
		task.signal(TaskScheduled, when=date_time)

	# Main loop
	for event in Message.objects(processed=False).tail(timeout):
		if not Message.objects(id=event.id, processed=False).update(set__processed=True):
			continue

		message_queue.put('Process %r' % event)

		if isinstance(event, StopRunner):
			message_queue.put('Runner is stopped')
			return

		if not isinstance(event, TaskMessage):
			continue

		task = event.task

		handler = {
			TaskScheduled: schedule_task,
			TaskAdded: add_task,
			TaskAddedRescheduled: add_task,
			ReschedulePeriodic: reschedule_periodic
		}.get(event.__class__, lambda task, event: None)

		if handler(task, event) is False:
			break