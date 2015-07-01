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


@pytest.fixture(scope='module')
def runner(request, connection):
	runner = Runner('./example/config.yaml')
	th = threading.Thread(target=runner.run)

	def finalizer():
		StopRunner.objects.create()
	request.addfinalizer(finalizer)

	th.daemon = True
	th.start()
	return runner


@pytest.fixture(scope="function")
def task(request):
	t = subject.defer(4)
	t.reload()

	def finalizer():
		Task.objects(id=t.id).delete()

	request.addfinalizer(finalizer)
	return t


class TestTasks(object):
	def test_result(self):
		assert subject(2) == 84
		assert subject.call(2) == 84

	def test_defer(self, task):
		assert task.state == 'pending'
		assert task.handle() == 168

	def test_defer_int(self, runner, task):
		assert int(task) == 168

	def test_defer_str(self, runner, task):
		assert str(task) == "168"

	def test_runner(self, runner):
		result = subject2(42)
		assert str(result) == "String 42"

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
