# encoding: utf-8


import logging
import logging.config
from functools import partial
from copy import deepcopy
from datetime import datetime

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
								 TaskScheduled, ReschedulePeriodic)
from marrow.task.exc import AcquireFailed
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


_runners = {}

_scheduler = BackgroundScheduler(timezone=utc)
_scheduler.start()


def _run_scheduled_job(task_id):
	print('START SCHEDULED')
	from marrow.task.model import Task
	# print(task_id)
	task = Task.objects.get(id=task_id)
	task.signal(TaskAdded)
	# print(Task.objects.get(id=task_id))
	# print('OLOAKSDAJIBHEJNTKWTGJSDG')
	# print("FR:", task.time.frequence)
	if not task.time.frequency:
	# 	task.signal(TaskAdded)
		return
	if task.time.until:
		if datetime.now().replace(tzinfo=utc) >= task.time.until.replace(tzinfo=utc):
			return
	task.signal(ReschedulePeriodic, when=datetime.now().replace(tzinfo=utc))
	# new_task = task.duplicate()
	# # date_time = (task.time.scheduled + (task.time.frequence - task.time.EPOCH)).replace(tzinfo=utc)
	# # import ipdb; ipdb.set_trace()
	# date_time = task.time.scheduled.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
	# new_task.time.scheduled = date_time
	# new_task.save()
	# # print(new_task.id)
	# new_task.signal(TaskScheduled, when=date_time)


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

	def schedule_task(self, task, message):
		from apscheduler.triggers.date import DateTrigger
		# from apscheduler.triggers.interval import IntervalTrigger

		# if task.time.frequency is None:
		trigger = DateTrigger(run_date=task.time.scheduled)
		# else:
		# 	from pytz import utc
		# 	delta = task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH
		# 	trigger = IntervalTrigger(seconds=delta.total_seconds(), start_date=task.time.scheduled, end_date=task.time.until)

		task_id = unicode(task.id)
		_scheduler.add_job(_run_scheduled_job, trigger=trigger, id=task_id, args=[task_id])

		self.logger.info("%s scheduled at %s" % (task_id, str(trigger)))

	def add_task(self, task, message):
		if task.acquire() is None:
			self.logger.warning("Failed to acquire lock on task: %r", task)
			return

		self.logger.info("Acquired lock on task: %r", task)
		r = RunningTask(self, task)
		_runners[unicode(task.id)] = r
		self.executor.submit(partial(_process_task, unicode(task.id)))

	def iterate_task(self, task, message):
		runner = _runners.get(unicode(task.id))
		if runner is not None:
			runner.next()

	def reschedule_periodic(self, task, message):
		from marrow.task.model import Task
		date_time = message.when.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
		Task.objects(id=task.id).update(set__time__scheduled=date_time)
		task.signal(TaskScheduled, when=date_time)

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

			if isinstance(event, TaskScheduled):
				self.schedule_task(task, event)

			elif isinstance(event, TaskAdded):
				self.add_task(task, event)

			elif isinstance(event, IterationRequest):
				self.iterate_task(task, event)

			elif isinstance(event, ReschedulePeriodic):
				self.reschedule_periodic(task, event)