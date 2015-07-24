# encoding: utf-8


import os
import logging
import logging.config
from functools import partial
from copy import deepcopy
from datetime import datetime
from multiprocessing import Queue
from multiprocessing.managers import BaseManager, DictProxy

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


_scheduler = BackgroundScheduler(timezone=utc)
_scheduler.start()


class RunnersStorage(object):
	def __init__(self):
		self._runners = _manager.dict()

	def __getitem__(self, item):
		return self._runners[item]

	def __setitem__(self, key, value):
		self._runners[key] = value

	def __delitem__(self, key):
		del self._runners[key]

	def get(self, key, default_value=None):
		return self._runners.get(key, default_value)


def _run_scheduled_job(task_id):
	from marrow.task.model import Task

	task = Task.objects.get(id=task_id)
	task.signal(TaskAdded)

	if not task.time.frequency:
		return

	if task.time.until:
		if datetime.now().replace(tzinfo=utc) >= task.time.until.replace(tzinfo=utc):
			return

	task.signal(ReschedulePeriodic, when=datetime.now().replace(tzinfo=utc))


class RunningTask(object):
	def __init__(self, task_id):
		self.task_id = task_id

	def handle_task(self):
		from marrow.task import task as task_decorator

		task = self.get_task()

		if not task.set_running_or_notify_cancel():
			return

		func = task.callable
		if not hasattr(func, 'context'):
			func = task_decorator(func)

		context = self.get_context(task)
		for key, value in iteritems(context):
			setattr(func.context, key, value)

		result = None
		try:
			result = task.handle()
		except Exception:
			# self.runner.logger.warning('Failed: %r', task)
			pass
		# else:
		# 	self.runner.logger.info('Completed: %r', task)

		return result

	def handle(self):
		self.handle_task()
		del _runners[self.task_id]

	def get_context(self, task):
		return dict(
			id = task.id,
		)

	def get_task(self):
		from marrow.task.model import Task
		return Task.objects.get(id=self.task_id)

_manager.register('RunningTask', RunningTask)


class RunningRescheduled(RunningTask):
	def handle(self):
		from marrow.task.model import Task
		Task.objects(id=self.task_id).update(set__time__completed=None)
		return super(RunningRescheduled, self).handle()

_manager.register('RunningRescheduled', RunningRescheduled)


class RunningGenerator(RunningTask):
	def handle(self):
		generator = self.handle_task()
		task = self.get_task()
		for event in IterationRequest.objects(task=self.task_id).tail():
			try:
				next(generator)
			except StopIteration:
				del _runners[self.task_id]
				break
			else:
				task._invoke_callbacks()

_manager.register('RunningGenerator', RunningGenerator)


_manager.start()

_runners = RunnersStorage()


def _process_task(task_id, errors_queue=None):
	try:
		_runners[task_id].handle()
	except Exception:
		if not errors_queue:
			return
		import sys, traceback
		exc_type, exc_val, tb = sys.exc_info()
		tb = ''.join(traceback.format_tb(tb))
		errors_queue.put((exc_type, exc_val, tb, task_id))


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

	def schedule_task(self, task, message):
		from apscheduler.triggers.date import DateTrigger

		trigger = DateTrigger(run_date=task.time.scheduled)
		task_id = unicode(task.id)
		_scheduler.add_job(_run_scheduled_job, trigger=trigger, id=task_id, args=[task_id])

		self.logger.info("%s scheduled at %s" % (task_id, str(trigger)))

	def add_task(self, task, message):
		if task.acquire() is None:
			self.logger.warning("Failed to acquire lock on task: %r", task)
			return

		self.logger.info("Acquired lock on task: %r", task)
		if isinstance(message, TaskAddedRescheduled):
			runner_class = RunningRescheduled
		else:
			if task.generator:
				runner_class = RunningGenerator
			else:
				runner_class = RunningTask
		runner = getattr(_manager, runner_class.__name__)(unicode(task.id))
		_runners[unicode(task.id)] = runner
		self.executor.submit(partial(_process_task, unicode(task.id), self.errors))

	def reschedule_periodic(self, task, message):
		from marrow.task.model import Task
		date_time = message.when.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
		Task.objects(id=task.id).update(set__time__scheduled=date_time, set__time__acquired=None, set__owner=None)
		task.signal(TaskScheduled, when=date_time)

	def run(self):
		for event in self.queryset.tail(timeout=self.timeout):
			while True:
				try:
					exc_type, exc_val, tb, task_id = self.errors.get(False)
				except queue_module.Empty:
					break
				error_msg = '%s(%s) in task %s:\n%s' % (exc_type.__name__, str(exc_val), task_id, tb)
				self.logger.exception(error_msg)

			if event.processed:
				continue
			event.process()

			msg = 'Process %s' % event.__class__.__name__
			if isinstance(event, TaskMessage):
				msg += ' for %s' % event.task.id
			self.logger.info(msg)

			if isinstance(event, StopRunner):
				self.logger.info("Runner is stopped")
				event.process()
				return

			if not isinstance(event, TaskMessage):
				continue

			task = event.task

			{
				TaskScheduled: self.schedule_task,
				TaskAdded: self.add_task,
				ReschedulePeriodic: self.reschedule_periodic
			}.get(event.__class__, lambda task, event: None)(task, event)