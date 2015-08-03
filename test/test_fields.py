# encoding: utf-8

from __future__ import unicode_literals, print_function

import pytest
from mongoengine.errors import ValidationError

from marrow.task.field import PythonReferenceField


def subject():
	return 42


@pytest.fixture(scope='function')
def field():
	return PythonReferenceField()


class TestPythonReferenceField(object):
	def test_to_python(self, field):
		assert field.to_python(subject) is subject
		assert field.to_python('test.test_fields:subject') is subject

	def test_to_mongo(self, field):
		assert field.to_mongo(subject) == 'test.test_fields:subject'

	def test_validate(self, field):
		assert field.validate(subject) is None
		with pytest.raises(ValidationError):
			field.validate(42)

	def test_prepare_query_value(self, field):
		assert field.prepare_query_value(None, 42) is 42
		assert field.prepare_query_value(None, subject) == 'test.test_fields:subject'
