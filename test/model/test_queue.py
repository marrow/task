# encoding: utf-8

from __future__ import unicode_literals

from time import time, sleep
from pytest import fixture
from random import choice
from threading import Thread
from mongoengine import Document, StringField, IntField

from marrow.task.model.query import CappedQuerySet


class Log(Document):
	meta = dict(queryset_class=CappedQuerySet, max_documents=100, max_size=2000000)
	
	message = StringField()
	priority = IntField(default=0)
	
	COUNT = 0


@fixture(scope='function', autouse=True)
def purge(request):
	request.addfinalizer(Log.drop_collection)


def pytest_funcarg__message(request):
	Log.COUNT += 1
	return Log('test #' + str(Log.COUNT)).save()


_PRIORITY = (-2, -1, 0, 1, 2)

def gen_log_entries(count=1000):
	for i in range(count-1):
		Log('test #' + str(i) + ' of ' + str(count), choice(_PRIORITY)).save()
	
	Log('last').save()


class TestCappedQueries(object):
	def test_single(self, message):
		assert Log.objects.count() == 1
		assert next(Log.objects.tail()).id == Log.objects.first().id
	
	def test_list_timeout(self, message):
		start = time()
		result = list(Log.objects.tail(timeout=2))
		delta = time() - start
		assert 1.9 < delta < 2.1  # Small fudge factor.
		
		assert len(result) == Log.objects.count()
	
	def test_loop_timeout(self, message):
		start = time()
		for record in Log.objects.tail(timeout=2):
			pass
		delta = time() - start
		assert 1.9 < delta < 2.1  # Small fudge factor.
	
	def test_capped_trap(self, message):
		assert Log._get_collection().options().get('capped', False)
		
		Log._get_collection().drop()  # bypass MongoEngine here
		
		assert not Log._get_collection().options().get('capped', False)
		
		try:
			list(Log.objects.tail())
		except TypeError:
			pass
		else:
			assert False, "Failed to raise an exception."
	
	def test_long_iteration(self):
		assert not Log.objects.count()
		
		# Start generating entries.
		Thread(target=gen_log_entries).start()
		
		count = 0
		for record in Log.objects.tail():
			count += 1
			if record.message == 'last': break
		
		# Note that the collection only allows 100 entries...
		assert Log.objects.count() == 100
		
		# But we successfully saw all 1000 generated records.  :)
		assert count == 1000
	
	def test_intermittent_iteration(self):
		assert not Log.objects.count()
		
		# Start generating entries.
		Thread(target=gen_log_entries).start()
		
		count = 0
		seen = None
		for record in Log.objects.tail(timeout=0.5):
			# Make sure after we pause that we continue from where we left off.
			if count == 100:
				assert record.id > seen
			
			count += 1
			
			# This will put quite the kink in our query.
			# Specifically, the original query will have timed out and a new one
			# will have to be generated.
			if count == 100:
				sleep(1)
				seen = record.id
			
			if record.message == 'last': break
		
		assert Log.objects.count() == 100
		assert count == 200  # The rest generated during that one second we paused.
