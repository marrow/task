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
def test(a, carg=None):
	print('Jesus')
	# import time
	# time.sleep(5)
	return 42 * a


# if not Task.objects:
#t = test.defer(2)
#print('>>> TASK: %s' % t.id)

# t = list(Task.objects)[-1]

import threading
runner = Runner('./example/config.yaml')

th = threading.Thread(target=runner.run)

# Use `runner.stop_test_runner` at end of the test for ensure that runner thread is stopped.
# Add it as finalizer for same at failures.
def stop(wait=None):
	runner.shutdown(wait)
	th.join()

runner.stop_test_runner = stop
# request.addfinalizer(stop)

#th.start()
t = test.defer(42).result
#runner.stop_test_runner()
# return runner

#runner.run()
