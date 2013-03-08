# encoding: utf-8

"""Basic sample tasks.

To use these, be sure to start an interactive shell and import the module.
"""

from __future__ import print_function


from marrow.tasks import task, Task


@task
def hello(name):
	return "Hello, " + name + " I'm " + hello.context.id


def farewell(name):
	return "Farewell " + name + "."


class Phrases(Task):
	def hello(self, name):
		print("Hello, ", name, ".", sep="")
	
	def farewell(self, name):
		print("Farewell ", name, ".", sep="")


def run():
	# Runs immediately in the current thread.
	# The result will always be available right away.
	print(hello("world").result)
	
	# The above is the same, by default, as: hello.call("world")
	
	# Runs in a background thread.
	# Getting the result blocks pending completion of the task, but you can also explicitly wait.
	receipt = hello.defer("world")
	receipt.wait()  # Wait forever for completion.
	receipt.wait(30)  # Wait 30 seconds.
	
	receipt.waiting  # True if the task hasn't been picked up by a worker yet.
	receipt.running  # True if the task is still processing.
	receipt.completed  # True if the task has finished.
	receipt.successful  # True if the task exited cleanly.
	receipt.failed  # True if there was an exception raised.
	
	print(receipt.result)
	
	# If there was an exception originally raised, it will be re-raised if you try to get the result.
	# You can manually check for the presence of receipt.exception (or just receipt.failed).
	
	# You can even use arbitrary dot-colon function references.
	print(task('basic:farewell').call("cruel world").result)
	
	
	Task.name  # The dot-colon notation path to the task.
	Task.context  # The current task context, if currently being executed.
	Task.abstract  # A base class for another task type; can not be executed.
	Task.retries  # The maximum number of retries.
	Task.delay  # The number of seconds to wait before starting execution.
	Task.rate  # The maximum number of this task to process.  (10, 2, 'hour'), 10 every two hours.
	Task.