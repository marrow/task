# coding: utf-8
from __future__ import unicode_literals

from mongoengine import connect
import concurrent.futures

from marrow.task import Task, task
from marrow.task.message import *
from marrow.task.runner import Runner
from marrow.task.future import TaskFuture

from example.runner import run
from example.basic import hello, count, farewell


connect('mtask')


@task
def test(a):
	print('Jesus')
	import time
	time.sleep(5)
	return 42 * a

def stop_runner():
	import time; time.sleep(10)
	runner.interrupt()

def test_concurrency():
	with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
		for i in xrange(10):
			t = hello.defer('Jesus-%s' % i)
			ex.submit(t.handle)

@task
def test_task(ta):
    return ta

@task
def test_map(god):
	return 'Hail, %s!' % god

gods = ['Baldur', 'Bragi', 'Eostre', 'Hermodur']

from mongoengine import Document

class TT(Document):
	def __iter__(self):
		raise AttributeError('Jesus')

	def save(self):
		return self.t()

	def t(self):
		import inspect
		return inspect.stack()[1]

@task
def test_exc():
	t = TT()
	for i in t:
		return


if not Task.objects:
	test.defer(2)

t = list(Task.objects)[-1]

runner = Runner('./example/config.yaml')
