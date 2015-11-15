# encoding: utf-8

import logging

try:
	from logging.config import dictConfig
except ImportError:
	from marrow.task.compat import dictConfig

from copy import deepcopy
from datetime import datetime
from multiprocessing import Manager, Queue

from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from yaml import load

try:
	from yaml import CLoader as Loader
except ImportError:
	from yaml import Loader

from mongoengine import connect, DoesNotExist
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from marrow.task.message import (TaskAdded, Message, IterationRequest, TaskMessage,
								 TaskScheduled, ReschedulePeriodic, TaskAddedRescheduled, TaskCompletedPeriodic)
from marrow.task.compat import str, unicode, iterkeys, iteritems, itervalues


LISTENED_MESSAGES = [
	TaskAdded,
	TaskScheduled,
	TaskAddedRescheduled,
	ReschedulePeriodic
]

LISTENED_MESSAGES = [cls._class_name for cls in LISTENED_MESSAGES]


HALT = 'HALT'


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


_manager = None


global_message_queue = None


def _initialize():
	global _manager

	if _manager is not None:
		return

	_manager = Manager()


def _run_periodic_task(task_id):
	"""Called by scheduler. Signal that task should be executed. If task is not expired, reschedule next iteration."""

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


def _process_exception(runner=False, exctb=None):
	import sys, traceback

	exc_type, exc_val, tb = sys.exc_info()
	if isinstance(exc_val, DoesNotExist):
		exc_val = DoesNotExist(*exc_val.args)
	if exctb is None:
		exctb = ''.join(traceback.format_tb(tb.tb_next))
	exc = (exc_type, exc_val, exctb)

	return RunStatus(RUNNER_EXCEPTION if runner else EXCEPTION, exc)


class RunningTask(object):
	"""Task handler. Created by runner thread/process. Provide task's context and initiate task execution."""

	def __init__(self, task):
		self.task = task

	def handle_task(self):
		from marrow.task import task as task_decorator

		if not self.task.set_running_or_notify_cancel():
			return RunStatus(FAILURE)

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
			exc = self.task.exception
			return _process_exception(exc is None, exctb=getattr(exc, 'original_traceback', None))

		return RunStatus(SUCCESS, result)

	def handle(self):
		return self.handle_task()

	def get_context(self, task):
		return dict(
			id = task.id,
		)


class RunningRescheduled(RunningTask):
	"""Handle rescheduled tasks."""

	def handle(self):
		self.task.acquire()
		result = super(RunningRescheduled, self).handle()
		self.task.release()
		return result


class RunningGenerator(RunningTask):
	"""Handle generator tasks. Evaluate task's generator and call task's iteration callbacks."""

	def handle(self):
		result = self.handle_task()
		if result.kind != SUCCESS:
			return result

		generator = result.data
		for item in generator:
			self.task._invoke_callbacks()
		return RunStatus(SUCCESS, None)


class RunningGeneratorWaiting(RunningGenerator):
	"""Handle generator tasks with `wait_for_iteration` option set to True.

	Wait for `IterationRequest` before evaluation of next generator's iteration."""

	def handle(self):
		result = self.handle_task()
		if result.kind != SUCCESS:
			return result

		generator = result.data
		for event in IterationRequest.objects(task=self.task).tail():
			try:
				next(generator)
			except StopIteration:
				break
			else:
				self.task._invoke_callbacks()
		return RunStatus(SUCCESS, None)


def _process_task(runner):#, message_queue=None):
	try:
		result = runner.handle()
	except Exception:
		result = _process_exception(True)

	if result is None:
		return
	# if message_queue is None or result is None:
	# 	return

	if result.kind in (EXCEPTION, RUNNER_EXCEPTION):
		exc_type, exc_val, tb = result.data
		return (exc_type, exc_val, tb, runner.task.id, result.kind)

	# else:
	return (result.kind, runner.task.id)


class Runner(object):
	"""Runner that execute tasks.

	All tasks running through executor that either instance of `ThreadPoolExecutor` or `ProcessPoolExecutor`.
	Executor type selected by config `runner.use` key.
	`runner.timeout` used to restrict runner's waiting time for new messages.
	All other `runner` key-value pairs passed to executor's constructor.

	`database` subdict passed entirely to PyMongo's `connect`.

	`logging` subdict passed to python's logging configuration.

	*Attributes:*

	* **message_queue** -- queue that used for passing log information from execution threads/processes to runner.
	* **message_thread** -- thread that evaluate `runner._handle_messages` and processes messages in `message_queue`.
	"""

	def __init__(self, config=None):
		_initialize()

		global global_message_queue
		global_message_queue = Queue()

		config = self._get_config(config)

		self._executor_class = dict(
			thread = ThreadPoolExecutor,
			process = ProcessPoolExecutor,
		)[config['runner'].pop('use')]
		self._executor_config = config['runner']

		self.timeout = config['runner'].pop('timeout')
		self.executor = None

		self._connection = None
		self._connect(config['database'])

		dictConfig(config['logging'])
		# Get first logger name from config or class name.
		logger_name = next(iterkeys(config['logging'].get('loggers', {self.__class__.__name__: None})))
		self.logger = logging.getLogger(logger_name)

		self.message_queue = None
		self.message_thread = None

		self.run_flags = []

	def _handle_messages(self):
		"""In infinite loop peek messages from queue and output it though logger."""

		while True:
			try:
				data = global_message_queue.get(True)
			except (IOError, EOFError, OSError):
				return
			except TypeError:
				continue

			if data is None:
				return

			if isinstance(data, (str, unicode)):
				if data == HALT:
					self.shutdown()
					return

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
				'runner at ' if exc_kind == RUNNER_EXCEPTION else '',
				task_id, tb)
			self.logger.error(error_msg)

	def run(self):
		"""Run runner.

		Instantiate executor with given config. Setup message queue and message processing thread.
		Run execution threads/processes with `run` function."""

		import ctypes

		if self.get_alive_workers_count():
			raise RuntimeError('Runner is already running')

		self.executor = self._executor_class(**self._executor_config)

		for i in range(self.executor._max_workers):
			flag = _manager.Value(ctypes.c_bool, True)
			self.run_flags.append(flag)
			self.executor.submit(run, timeout=self.timeout, message_queue=None, run_flag=flag)

		self._handle_messages()

	@staticmethod
	def _get_config(config):
		"""Get runner's config.

		If `config` is None, return default config.
		If `config` is dict update default config by it.
		If `config` is string, open that file.
		If `config` is file, it must be a valid YAML. Load it and merge with default config."""

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
		"""Connect to MongoDB with given config."""

		if self._connection:
			return
		self._connection = connect(**config)

	def get_alive_workers_count(self):
		"""Return count of currently alive executor threads/processes."""

		if self.executor is None:
			return 0

		try:
			workers = self.executor._threads
		except AttributeError:
			workers = self.executor._processes

		if not workers:
			return 0

		result = 0

		for worker in (itervalues(workers) if isinstance(workers, dict) else workers):
			try:
				result += int(worker.is_alive())
			except AssertionError:
				continue

		return result

	def shutdown(self, wait=None):
		"""Shutdown runner."""

		for flag in self.run_flags:
			try:
				flag.value = False
			except Exception:
				continue

		try:
			global_message_queue.put(None)
		except Exception as e:
			print(e)
		try:
			global_message_queue.close()
		except Exception as e:
			print(e)
		try:
			global_message_queue.cancel_join_thread()
		except Exception as e:
			print(e)
		self.executor.shutdown(wait=True)
		# self.message_thread.join()
		self.logger.info("SHUTDOWN")


def run(timeout=None, message_queue=None, run_flag=None):
	"""Main execution loop.

	Instantiate scheduler for scheduled and periodic tasks.
	Listen for messages enumerated in `LISTENED_MESSAGES` and process it.

	:param timeout: timeout for message listening. Loop exit if no new messages within timeout.
	:param message_queue: queue to which task handlers put log messages.
	:param run_flag: boolean `multiprocessing.managers.Value` instance used for queryset interruption."""

	message_queue = global_message_queue

	scheduler = BackgroundScheduler(timezone=utc)
	scheduler.start()

	def schedule_task(task, message):
		from apscheduler.triggers.date import DateTrigger

		if task.cancelled:
			return

		trigger = DateTrigger(run_date=task.time.scheduled)
		task_id = unicode(task.id)
		scheduler.add_job(_run_periodic_task, trigger=trigger, id=task_id, args=[task_id])

	def add_task(task, message):
		if task.acquire() is None:
			return

		if isinstance(message, TaskAddedRescheduled):
			runner_class = RunningRescheduled
		else:
			if task.generator:
				if task.options.get('wait_for_iteration'):
					runner_class = RunningGeneratorWaiting
				else:
					runner_class = RunningGenerator
			else:
				runner_class = RunningTask

		runner = runner_class(task)
		result = None
		try:
			result = _process_task(runner)
		except RuntimeError:
			return False

		if message_queue and result is not None:
			message_queue.put(result)

	def reschedule_periodic(task, message):
		from marrow.task.model import Task
		date_time = message.when.replace(tzinfo=utc) + (task.time.frequency.replace(tzinfo=utc) - task.time.EPOCH)
		if not Task.objects(id=task.id, time__cancelled=None).update(set__time__scheduled=date_time, set__time__acquired=None, set__owner=None):
			return
		task.signal(TaskScheduled, when=date_time)

	# Main loop
	# import signal

	# queryset = Message.objects(__raw__={'_cls': {'$in': LISTENED_MESSAGES}}, processed=False)
	queryset = Message.objects()
	queryset._flag = run_flag

	# def inner_handler(s, f):
	# 	pass

	# try:
	# 	signal.signal(signal.SIGINT, inner_handler)
	# except ValueError:
	# 	print('UNSUCCESSFULL')
	# 	pass
		# querysets.append(queryset)

	for event in queryset.tail(timeout):
		# import random
		# import time; time.sleep(random.random())
		if not Message.objects(id=event.id, processed=False).update(set__processed=True):
			continue

		# message_queue.put('Process %r' % event)

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
	message_queue.put(HALT)


def default_runner():
	# import signal
	import sys
	import os.path

	config = sys.argv[1] if len(sys.argv) > 1 else None
	if config is not None:
		config = os.path.expanduser(os.path.expandvars(config))

		if not os.path.exists(config):
			sys.exit('Error: Config file is not exists.')

		if not os.path.isfile(config):
			sys.exit('Error: Config must be a file.')

	runner = Runner(config)

	def handler(sig, fr):
		import sys
		sys.exit(0)
		# runner.shutdown()

	# signal.signal(signal.SIGINT, handler)

	try:
		runner.run(block=True)
	finally:
		# import ipdb; ipdb.set_trace()
		runner.shutdown()

	# if isinstance(runner.executor, ProcessPoolExecutor):
	# 	ex = runner.executor._processes
	# 	for p in ex:
	# 		p.join()
	# else:
	# 	while any(th.is_alive() for th in runner.executor._threads):
	# 		import time; time.sleep(0.1)

