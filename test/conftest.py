# encoding: utf-8

import pytest
import mongoengine

from functools import partial
from marrow.task.message import Keepalive


@pytest.fixture(scope="module", autouse=True)
def connection(request):
	"""Automatically connect before testing and discard data after testing."""
	connection = mongoengine.connect('testing')
	connection.testing.drop_collection("TaskQueue")
	Keepalive.objects.create()

	request.addfinalizer(partial(connection.drop_database, 'testing'))
	return connection
