# encoding: utf-8

import os
import math
import pytest

from itertools import chain
from textwrap import TextWrapper

from marrow.task.util import resolve, lookup


class TestResolve(object):
	def test_basic(self):
		assert resolve(chain) == 'itertools:chain'
	
	def test_class(self):
		assert resolve(TextWrapper) == 'textwrap:TextWrapper'
	
	@pytest.mark.skipif(bool(os.environ.get('CANARY', False)), reason="No im_class or __qualname__ support.")
	def test_method(self):
		assert resolve(TextWrapper.fill) == 'textwrap:TextWrapper.fill'
	


class TestLookup(object):
	def test_basic(self):
		assert lookup('math:pi') == math.pi
