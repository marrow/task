# encoding: utf-8

import os
import math
import pytest

from itertools import chain
from textwrap import TextWrapper

from marrow.task.util import resolve, load


canaried = pytest.mark.skipif(bool(os.environ.get('CANARY', False)), reason="No im_class or __qualname__ support.")


class TestResolveStdlib(object):
	def test_basic(self):
		assert resolve(chain) == 'itertools:chain'
	
	def test_class(self):
		assert resolve(TextWrapper) == 'textwrap:TextWrapper'
	
	@canaried
	def test_method(self):
		assert resolve(TextWrapper.fill) == 'textwrap:TextWrapper.fill'


class TestResolveMarrowTask(object):
	def test_class(self):
		from marrow.task.model.task import Task
		assert resolve(Task) == 'marrow.task.model.task:Task'
	
	@canaried
	def test_method(self):
		from marrow.task.model.task import Task
		assert resolve(Task().acquire) == 'marrow.task.model.task:Task.acquire'
		assert resolve(Task.acquire) == 'marrow.task.model.task:Task.acquire'
	
	@canaried
	def test_class_method(self):
		from marrow.task.model.task import Task
		assert resolve(Task.submit) == 'marrow.task.model.task:Task.submit'
		assert resolve(Task().submit) == 'marrow.task.model.task:Task.submit'


class TestLookup(object):
	def test_basic(self):
		assert load('math:pi') == math.pi
	
	def test_class(self):
		from marrow.task.model.task import Task
		assert load('marrow.task.model.task:Task') is Task
	
	@canaried
	def test_method(self):
		from marrow.task.model.task import Task
		assert load('marrow.task.model.task:Task.acquire') is Task.acquire
	
	@canaried
	def test_class_method(self):
		from marrow.task.model.task import Task
		assert load('marrow.task.model.task:Task.submit') == Task.submit
