# encoding: utf-8

import pytest
import mongoengine

from functools import partial


@pytest.fixture(scope="function", autouse=True)
def connection(request):
	"""Automatically connect before testing and discard data after testing."""
	connection = mongoengine.connect('testing')
	request.addfinalizer(partial(connection.drop_database, 'testing'))
	return connection
