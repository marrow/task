# encoding: utf-8

from __future__ import unicode_literals, print_function

import time

from mongoengine import connect
from marrow.task import task

from tasks import hello, count, emit


def run():
	connect('mtask')

	print(hello("Bob Dole"))
	print(hello.call("Al Gore"))
	
	receipt = hello.defer("world")

	receipt.wait()  # Wait forever for completion (of any kind).
	receipt.wait(30)  # Wait 30 seconds.
	
	# The completion messages contain the results, so waiting will update the local task instance with the result
	# or exception as appropriate, plus the waiting/running/completed/successful/failed times and thus booleans.
	
	receipt = receipt.reload()
	
	print(receipt.state)
	print("waiting", receipt.waiting)  # True if the task hasn't been picked up by a worker yet.
	print("running", receipt.running)  # True if the task is still processing.
	print("done", receipt.done)  # True if the task has finished.
	print("successful", receipt.successful)  # True if the task exited cleanly.
	print("failed", receipt.failed)  # True if there was an exception raised.
	print("performance", receipt.performance)  # Various performance statistics.
	
	print("result =", receipt.result)  # Finally emit the result. If not finished, wait forever for the result.
	
	receipt = count(10)
	receipt.add_done_callback(emit)
	
	time.sleep(15)
	
	# There are some useful helpers:
	
	receipt = hello.defer("world")
	print(receipt)  # str()
	
	for i in count(10):
		print(i)  # same, but progressively, as each yield is issued by the task


if __name__ == '__main__':
	connect('mtask')
	run()
	