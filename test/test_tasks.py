# encoding: utf-8

from __future__ import unicode_literals, print_function

from time import sleep

import pytest
from mongoengine import Document, IntField, ReferenceField

from marrow.task import task as task_decorator
from marrow.task import Task
from marrow.task.compat import range, py33, total_seconds
from marrow.task.exc import TimeoutError


class ModelForTest(Document):
	task = ReferenceField(Task)
	data_field = IntField()

	def method(self, arg):
		return self.data_field * arg


def check(predicate, expected, min=0, max=0):
	count = 0
	sleep(min)
	limit = 10 if max == 0 else (max - min) * 2
	result = None
	while count < limit:
		sleep(0.5)
		result = predicate()
		if result == expected:
			return True
		count += 1
	raise ValueError('%r != %r' % (result, expected))


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


@task_decorator(wait=True)
def waiting_generator_subject():
	for i in range(10):
		yield i


@task_decorator
def map_subject(god):
	return "Hail, %s!" % god


@task_decorator
def sleep_subject(a, sleep=1):
	import time
	time.sleep(sleep)
	return a


def task_callback(task):
	result = "Callback for %s" % task.id
	ModelForTest.objects(task=task).update(inc__data_field=1)
	return result


@task_decorator
def context_subject():
	return context_subject.context.id


@task_decorator
def exception_subject():
	raise AttributeError('FAILURE')


@task_decorator
def every_subject():
	ModelForTest.objects(task=every_subject.context.id).update(inc__data_field=1)
	return ModelForTest.objects(task=every_subject.context.id).scalar('data_field').first()


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
		assert subject(2).result == 84
		assert subject.call(2).result == 84

	def test_defer(self, task):
		assert task.state == 'pending'
		assert task.handle() == 168

	def test_defer_int(self, runner, task):
		assert int(task) == 168

	def test_defer_str(self, runner, task):
		assert str(task) == "168"

	def test_defer_float(self, runner):
		task = subject.defer(14.2)
		assert float(task) == 596.4

	def test_runner(self, runner):
		result = subject2(42)
		assert str(result) == "String 42"

	def test_generator(self):
		assert list(generator_subject.defer().handle()) == list(range(10))

	def test_generator_task(self, runner):
		for i in range(runner.executor._max_workers + 2):
			task = generator_subject.defer(fail=False)
			assert list(task) == list(range(10))
			from marrow.task.message import TaskProgress
			count = TaskProgress.objects.count()
			assert list(task) == list(range(10))
			assert TaskProgress.objects.count() == count

	def test_generator_task_waiting(self, runner):
		task = waiting_generator_subject.defer()
		assert list(task) == list(range(10))
		assert task.result == list(range(10))
		task = waiting_generator_subject.defer()
		generator = task.iterator()
		from marrow.task.message import TaskProgress
		base_count = TaskProgress.objects.count()
		assert TaskProgress.objects.count() - base_count == 0
		assert next(generator) == 0
		assert next(generator) == 1
		assert TaskProgress.objects.count() - base_count == 2
		assert list(generator) == list(range(2, 10))
		assert task.result == list(range(10))

	@pytest.mark.skipif(not py33, reason="requires Python 3.3")
	def test_generator_task_exception_value(self, runner):
		task = generator_subject.defer(exc_val=42)
		assert list(task) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42]

	def test_generator_task_exception(self, runner):
		task = subject.defer(42)
		with pytest.raises(ValueError) as exc:
			list(task)
		assert 'Only periodic and generator tasks are iterable.' in str(exc)
		task = generator_subject.defer(fail=True)
		with pytest.raises(ValueError):
			list(task)
		assert 'FAILURE' in str(task.exception)

	def test_generator_task_iteration_callback(self, connection, runner):
		task = generator_subject.defer()
		ModelForTest.objects.create(task=task, data_field=0)
		task.add_callback(task_callback, iteration=True)
		assert_task(task)
		assert list(task) == list(range(10))
		assert check(lambda: ModelForTest.objects(task=task).scalar('data_field').first(), 10)
		task.add_callback(task_callback)
		assert check(lambda: ModelForTest.objects(task=task).scalar('data_field').first(), 11)

	def test_submit(self, runner):
		future = Task.submit(subject, 2)
		assert future.result() == 84
		future2 = Task.submit(sleep_subject, 42, 10)
		with pytest.raises(TimeoutError):
			future2.result(1)

	def test_map(self, runner):
		data = ['Baldur', 'Bragi', 'Ēostre', 'Hermóður']
		count = Task.objects.count()
		result = list(Task.map(map_subject, data))
		assert Task.objects.count() - count == len(data)
		assert result == ['Hail, %s!' % god for god in data]

	def test_map_timeout(self, runner):
		data = ['Baldur', 'Bragi', 'Ēostre', 'Hermóður']
		result = Task.map(sleep_subject, data, timeout=1)
		with pytest.raises(TimeoutError):
			list(result)

	def test_callback(self, connection, runner):
		task = sleep_subject.defer(42)
		ModelForTest.objects.create(task=task, data_field=0)
		task.add_callback(task_callback)
		assert_task(task)
		assert check(lambda: ModelForTest.objects(task=task).first().data_field, 1)

	def test_context(self, runner):
		task = context_subject.defer()
		assert_task(task)
		assert task.result == task.id

	def test_exception(self, runner):
		task = exception_subject.defer()
		with pytest.raises(AttributeError) as exc:
			task.result
		assert 'FAILURE' in str(exc.value)
		assert_task(task, 'failed')

	def test_acquire(self):
		task = Task.objects.create(callable=subject)

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
		instance = ModelForTest.objects.create(data_field=42)
		task = task_decorator(instance.method).defer(2)
		assert task.result == 84

	def test_at_invocation(self, runner):
		from datetime import datetime, timedelta
		import time

		dt = datetime.now() + timedelta(seconds=5)
		task = subject.at(dt, 2)
		start = time.time()
		assert task.result == 84
		assert abs(time.time() - start - 5) <= 1.0

	def test_every_invocation(self, connection, runner):
		from marrow.task.message import TaskComplete

		task = every_subject.every(3)
		ModelForTest.objects.create(task=task, data_field=0)
		assert check(lambda: task.result, 1, min=4)
		assert check(lambda: TaskComplete.objects(task=task).count(), 4, min=9, max=20)
		task.cancel()
		assert list(task) == [1, 2, 3, 4]
		assert task.result == 4

	def test_every_invocation_start_until(self, connection, runner):
		from datetime import datetime, timedelta

		from marrow.task.message import TaskComplete

		start = datetime.now() + timedelta(seconds=2)
		end = start + timedelta(seconds=6)
		task = every_subject.every(2, starts=start, ends=end)
		ModelForTest.objects.create(task=task, data_field=0)
		iterations_expected = int(total_seconds(end - start) // 2)
		task.wait(periodic=True)
		iterations_count = task.get_messages(TaskComplete).count()
		assert iterations_count <= iterations_expected
		assert list(task) == list(range(1, iterations_count + 1))