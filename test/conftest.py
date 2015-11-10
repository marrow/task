# encoding: utf-8

import pytest
import mongoengine

from functools import partial

from marrow.task.message import Message
from marrow.task.runner import Runner


@pytest.fixture(scope="module", autouse=True)
def connection(request):
	"""Automatically connect before testing and discard data after testing."""
	connection = mongoengine.connect('testing')
	connection.testing.drop_collection("TaskQueue")
	connection.testing.create_collection('test_data')
	try:
		connection.testing.create_collection(
			Message._meta['collection'],
			capped=True,
			size=Message._meta['max_size'],
			max=Message._meta['max_documents']
		)
	except Exception:
		pass

	from .test_tasks import ModelForTest
	ModelForTest.objects.create(data_field=0)

	request.addfinalizer(partial(connection.drop_database, 'testing'))
	return connection


@pytest.fixture(scope='module', params=['thread', 'process'], ids=['thread', 'process'])
def runner(request, connection):
	import threading
	config = Runner._get_config('./example/config.yaml')
	config['runner']['use'] = request.param
	# config['runner']['timeout'] = 10
	runner = Runner(config)
	rth = threading.Thread(target=runner.run)
	rth.start()
	# Use `runner.stop_test_runner` at end of the test for ensure that runner thread is stopped.
	# Add it as finalizer for same at failures.
	def stop(wait=None):
		print("TEST RUNNER SHUTDOWNS")
		runner.shutdown(True)
		rth.join()

	runner.stop_test_runner = stop
	request.addfinalizer(stop)

	return runner