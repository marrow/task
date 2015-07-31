# encoding: utf-8

from __future__ import unicode_literals, print_function

import threading
from multiprocessing import Manager

import pytest
from mongoengine import Document, IntField

from marrow.task import task as task_decorator
from marrow.task import Task
from marrow.task.compat import range, py2, py33


class TestModel(Document):
	data_field = IntField()

	def method(self, arg):
		return self.data_field * arg


@task_decorator
def subject(a):
	return 42*a


@task_decorator(defer=True)
def subject2(a):
	return "String %s" % a


@task_decorator
def generator_subject(fail=False, exc_val=None):
	for i in range(10):
		if fail and i == 5:
			raise ValueError('FAILURE')
		yield i
	if exc_val:
		raise StopIteration(exc_val)


@task_decorator
def map_subject(god):
	return "Hail, %s!" % god


@task_decorator
def sleep_subject(a):
	import time
	time.sleep(1)
	return a

_manager = Manager()
callback_count = _manager.Value('i', 0)


def task_callback(task):
	global callback_count

	result = "Callback for %s" % task.id
	print(result)
	callback_count.value += 1
	return result


@task_decorator
def context_subject():
	return context_subject.context.id


@task_decorator
def exception_subject():
	raise AttributeError('FAILURE')


every_count = _manager.Value('i', 0)


@task_decorator
def every_subject():
	every_count.value += 1
	return every_count.value


@pytest.fixture(scope="function")
def task(request, connection):
	t = subject.defer(4)
	t.reload()

	def finalizer():
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

	def test_defer_float(self, runner):
		task = subject.defer(14.2)
		assert float(task) == 596.4
		runner.stop_test_runner()

	def test_runner(self, runner):
		result = subject2(42)
		assert str(result) == "String 42"
		runner.stop_test_runner()

	def test_generator(self):
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

	def test_generator_task(self, runner):
		task = generator_subject.defer(fail=False)
		assert list(task) == list(range(10))
		from marrow.task.message import TaskIterated
		count = TaskIterated.objects.count()
		assert list(task) == list(range(10))
		assert TaskIterated.objects.count() == count
		runner.stop_test_runner(5)

	@pytest.mark.skipif(not py33, reason="requires Python 3.3")
	def test_generator_task_exception_value(self, runner):
		task = generator_subject.defer(exc_val=42)
		assert list(task) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42]
		runner.stop_test_runner(5)

	def test_generator_task_exception(self, runner):
		task = subject.defer(42)
		with pytest.raises(ValueError) as exc:
			list(task)
		assert 'Cannot use on non-generator tasks.' in str(exc)
		task = generator_subject.defer(fail=True)
		with pytest.raises(ValueError):
			list(task)
		assert 'FAILURE' in str(task.exception)
		runner.stop_test_runner(5)

	def test_map(self, runner):
		data = ['Baldur', 'Bragi', 'Ēostre', 'Hermóður']
		count = Task.objects.count()
		result = list(Task.map(map_subject, data))
		assert Task.objects.count() - count == len(data)
		assert result == ['Hail, %s!' % god for god in data]
		runner.stop_test_runner()

	def test_map_timeout(self, runner):
		from marrow.task.exc import TimeoutError

		data = ['Baldur', 'Bragi', 'Ēostre', 'Hermóður']
		result = Task.map(sleep_subject, data, timeout=1)
		with pytest.raises(TimeoutError):
			list(result)
		runner.stop_test_runner(True)

	def test_callback(self, runner):
		callback_count.value = 0
		task = sleep_subject.defer(42)
		task.add_done_callback(task_callback)
		assert_task(task)
		import time
		time.sleep(1)
		assert callback_count.value == 1
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

	def test_acquire(self, task):
		assert task.release() is None
		assert isinstance(task.acquire(), Task)
		assert task.acquire() is None
		assert isinstance(task.release(), Task)
		assert task.release() is None

		from marrow.task.structure import Owner
		test_owner = Owner.identity()
		test_owner.pid += 1

		task.acquire()
		assert task.owner is not None
		task.owner = test_owner
		task.save()

		assert task.release() is None
		assert isinstance(task.release(force=True), Task)

	def test_instance_method(self, runner):
		instance = TestModel.objects.create(data_field=42)
		task = task_decorator(instance.method).defer(2)
		assert task.result == 84
		runner.stop_test_runner()

	def test_at_invocation(self, runner):
		from datetime import datetime, timedelta
		import time

		dt = datetime.now() + timedelta(seconds=5)
		task = subject.at(dt, 2)
		start = time.time()
		assert task.result == 84
		assert time.time() - start >= 5
		runner.stop_test_runner()

	def test_every_invocation(self, runner):
		every_count.value = 0
		task = every_subject.every(3)
		for i in range(1, 4):
			# import time; time.sleep(6)
			assert task.result == i
		runner.stop_test_runner()

	def test_every_invocation_start_until(self, runner):
		from datetime import datetime, timedelta
		import time

		every_count.value = 0
		start = datetime.now() + timedelta(seconds=5)
		end = start + timedelta(seconds=6)
		total_start = time.time()
		task = every_subject.every(2, starts=start, ends=end)
		iterations = int((end - start).total_seconds() // 2)
		for i in range(1, iterations):
			iteration_start = time.time()
			time.sleep(2.2)
			assert task.result == i
			assert time.time() - iteration_start >= 2
		assert time.time() - total_start >= iterations * 2
		runner.stop_test_runner()
