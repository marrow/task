# encoding: utf-8

from __future__ import unicode_literals, print_function

import time

from marrow.task import task


@task
def hello(name):
	# Context is a (threading.locals) thread-local pool constructed by the decorator.
	# This context normally contains:
	#  * id - the ObjectId of the task being executed
	#  * task - a lazily-
	# Execution (immediate, deferred, whatever) provides the ObjectId representing this execution, or None
	# if running locally.
	time.sleep(1)
	res =  "Hello, %s I'm %s" % (name, hello.context.id or 'running locally')
	return res


@task(defer=True)
def count(n):
	for i in range(n):
		time.sleep(1)
		yield i


def emit(task):
	print(task.result)
