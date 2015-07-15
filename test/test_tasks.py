# encoding: utf-8

from __future__ import unicode_literals

import threading

import pytest

from marrow.task import task as task_decorator
from marrow.task import Task
from marrow.task.message import Keepalive, StopRunner
from marrow.task.runner import Runner


@task_decorator
def subject(a):
	return 42*a


@task_decorator(defer=True)
def subject2(a):
	return "String %s" % a


@task_decorator
def generator_subject():
	for i in range(10):
		yield i


@task_decorator
def map_subject(god):
	return "Hail, %s!" % god


@task_decorator
def sleep_subject(a):
	import time
	time.sleep(1)
	return a


def task_callback(task):
	result = "Callback for %s" % task.id
	print(result)
	task_callback.count += 1
	return result
task_callback.count = 0


@task_decorator
def context_subject():
	return context_subject.context.id


@task_decorator
def exception_subject():
	raise AttributeError('FAILURE')


@pytest.fixture(scope='function')
def runner(request, connection):
	runner = Runner('./example/config.yaml')
	th = threading.Thread(target=runner.run)

	# Use `runner.stop_test_runner` at end of the test for ensure that runner thread is stopped.
	# Add it as finalizer for same at failures.
	def stop():
		# StopRunner.objects.create()
		runner.interrupt()
		th.join()
	runner.stop_test_runner = stop
	request.addfinalizer(stop)

	th.start()
	return runner


@pytest.fixture(scope="function")
def task(request, connection):
	t = subject.defer(4)
	t.reload()
	print "Task created: %s" % t.id

	def finalizer():
		print('DELETED')
		Task.objects(id=t.id).delete()

	request.addfinalizer(finalizer)
	return t


def assert_task(task, state='complete'):
	assert task.wait().state == state


class TestTasks(object):
	def test_result(self):
		assert subject(2) == 84
		assert subject.call(2) == 84

	def test_defer(self, task):
		assert task.state == 'pending'
		assert task.handle() == 168

	def test_defer_int(self, runner, task):
		assert int(task) == 168
		runner.stop_test_runner()

	def test_defer_str(self, runner, task):
		assert str(task) == "168"
		runner.stop_test_runner()

	def test_runner(self, runner):
		result = subject2(42)
		assert str(result) == "String 42"
		runner.stop_test_runner()

	def test_generator(self, runner):
		from functools import partial

		gen = generator_subject.defer()

		def handle_generator(tid):
			ta = Task.objects.get(id=tid)
			iterator = ta.handle()
			list(iterator)

		handler = threading.Thread(target=partial(handle_generator, gen.id))
		handler.start()

		assert list(gen) == list(range(10))
		handler.join()
		runner.stop_test_runner()

	def test_map(self, runner):
		data = ['Baldur', 'Bragi', 'Ēostre', 'Hermóður']
		count = Task.objects.count()
		result = list(Task.map(map_subject, data))
		assert Task.objects.count() - count == len(data)
		assert result == ['Hail, %s!' % god for god in data]
		runner.stop_test_runner()

	def test_callback(self, runner):
		task = sleep_subject.defer(42)
		assert task_callback.count == 0
		task.add_done_callback(task_callback)
		assert_task(task)
		import time
		time.sleep(1)
		assert task_callback.count == 1
		runner.stop_test_runner()

	def test_context(self, runner):
		task = context_subject.defer()
		assert_task(task)
		assert task.result == task.id
		runner.stop_test_runner()

	def test_exception(self, runner):
		task = exception_subject.defer()
		with pytest.raises(AttributeError) as exc:
			task.result
		assert 'FAILURE' in str(exc.value)
		assert_task(task, 'failed')
		runner.stop_test_runner()