# encoding: utf-8

from __future__ import unicode_literals

from time import time, sleep
from pytest import fixture
from random import choice
from threading import Thread
from mongoengine import Document, StringField, IntField

from marrow.task.compat import str, unicode
from marrow.task.structure import Owner, Retry, Progress



class TestOwner(object):
	rec = Owner(host='tenthdimension', ppid=1, pid=1000)
	
	def test_encoded(self):
		assert self.rec.to_mongo().to_dict() == {'h': 'tenthdimension', 'P': 1, 'p': 1000}
	
	def test_repr(self):
		assert repr(self.rec) == 'Owner("tenthdimension", pid=1000, ppid=1)'
	
	def test_str(self):
		assert unicode(self.rec) == str(self.rec).decode('ascii') == 'tenthdimension:1:1000'
	
	def test_local(self):
		from os import getpid, getppid
		from socket import gethostname, gethostbyname
		
		rec = Owner.identity()
		
		assert rec.to_mongo().to_dict() == {'h': gethostbyname(gethostname()), 'P': getppid(), 'p': getpid()}


class TestRetry(object):
	pass


class TestProgress(object):
	pass
