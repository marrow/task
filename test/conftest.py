# encoding: utf-8

import pytest
import mongoengine

from functools import partial
from marrow.task.message import Message


@pytest.fixture(scope="function", autouse=True)
def connection(request):
	"""Automatically connect before testing and discard data after testing."""
	connection = mongoengine.connect('testing')
	connection.testing.drop_collection("TaskQueue")
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
