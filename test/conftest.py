# encoding: utf-8

import pytest
import mongoengine

from functools import partial

from marrow.task.message import Message
from marrow.task.runner import Runner


@pytest.fixture(scope="function", autouse=True)
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

	request.addfinalizer(partial(connection.drop_database, 'testing'))
	return connection


@pytest.fixture(scope='function', params=['thread', 'process'], ids=['thread', 'process'])
def runner(request, connection):
	config = Runner._get_config('./example/config.yaml')
	config['runner']['use'] = request.param
	config['runner']['timeout'] = 10
	runner = Runner(config)
	# th = threading.Thread(target=runner.run)
	runner.run()
	# Use `runner.stop_test_runner` at end of the test for ensure that runner thread is stopped.
	# Add it as finalizer for same at failures.
	def stop(wait=None):
		# import ipdb; ipdb.set_trace()
		runner.shutdown(True)
		# th.join()

	runner.stop_test_runner = stop
	request.addfinalizer(stop)

	# th.start()
	return runner