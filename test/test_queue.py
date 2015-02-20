# encoding: utf-8

from __future__ import unicode_literals

from time import time, sleep
from pytest import fixture
from random import choice
from threading import Thread
from mongoengine import Document, StringField, IntField

from marrow.task.queryset import CappedQuerySet


class Log(Document):
	meta = dict(queryset_class=CappedQuerySet, max_documents=10, max_size=1000000)
	
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

def gen_log_entries(count=100):
	Log('first').save()  # To avoid immediate exit of the tail.
	
	for i in range(count-2):
		sleep(0.02)  # If we go too fast, the test might not be able to keep up.
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
		assert 1.9 < delta < 5.1  # Fudged due to SERVER-15815
		
		assert len(result) == Log.objects.count()
	
	def test_loop_timeout(self, message):
		start = time()
		
		for record in Log.objects.tail(timeout=2):
			pass
		
		delta = time() - start
		assert 1.9 < delta < 5.1  # Fudged due to SERVER-15815
	
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
		t = Thread(target=gen_log_entries)
		t.start()
		
		count = 0
		for record in Log.objects.tail():
			count += 1
			if record.message == 'last': break
		
		# Note that the collection only allows 10 entries...
		assert Log.objects.count() == 10
		
		# But we successfully saw all 100 generated records.  :)
		assert count == 100
		
		t.join()
	
	def test_intermittent_iteration(self):
		assert not Log.objects.count()
		
		# Start generating entries.
		t = Thread(target=gen_log_entries)
		t.start()
		
		count = 0
		seen = None
		for record in Log.objects.tail(timeout=2):
			if count == 50:
				# Records are pooled in batches, so even after the query is timed out-i.e. we take
				# too long to do something in one of our iterations-we may still recieve additional
				# records before the pool drains and the cursor needs to pull more data from the
				# server.  To avoid weird test failures, we break early here.  Symptoms show up as
				# this test failing with "<semi-random large int> == 200" in the final count.
				# If you need to worry about this (i.e. having your own retry), track last seen.
				break
			
			count += 1
			seen = record.id
		
		for record in Log.objects(id__gt=seen).tail(timeout=2):
			count += 1
			if record.message == 'last': break
		
		t.join()
		
		assert Log.objects.count() == 10
		assert count == 100  # The rest generated during that one second we paused.
