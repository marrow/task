# encoding: utf-8

from mongoengine import connect
from marrow.task import Task
from marrow.task.message import TaskAcquired, TaskComplete


def run(tidx):
	connect('mtask')
	task = Task.objects.get(id=tidx)
	task.signal(TaskAcquired)
	# import ipdb; ipdb.set_trace()
	task.handle()
	# task.signal(TaskComplete, success=task.exception is None, result=task.result)
