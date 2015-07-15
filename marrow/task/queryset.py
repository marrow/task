# encoding: utf-8

from __future__ import print_function, unicode_literals

from time import time
from mongoengine import QuerySet, Q


class CappedQuerySet(QuerySet):
	"""A custom queryset that allows for tailing of capped collections."""

	def __init__(self, *args, **kwargs):
		super(CappedQuerySet, self).__init__(*args, **kwargs)
		self._running = True

	def interrupt(self):
		"""Set `_running` flag to False, thus stop fetching database after current iteration."""
		self._running = False

	def tail(self, timeout=None):
		"""A generator which will block and yield entries as they are added to the collection.
		
		Only use this on capped collections; ones with meta containing `max_size` and/or `max_documents`.
		
		Accepts the int/float `timeout` named argument indicating a number of seconds to wait for a result.  This
		value will be an estimate, not a hard limit, until https://jira.mongodb.org/browse/SERVER-15815 is fixed.  It will "snap" to the nearest multiple of the mongod process wait time.
		
		for obj in MyDocument.objects.tail():
			print(obj)
		
		Additional important note: tailing will fail (badly) if the collection is empty.  Always prime the collection
		with an empty or otherwise unimportant record before attempting to use this feature.
		"""
		
		# Process the timeout value, if one is provided.
		if timeout:
			end = time() + timeout
		
		# Prepare the query and extract often-reused values.
		q = self.clone()
		collection = q._collection
		query = q._query
		
		if not collection.options().get('capped', False):
			raise TypeError("Can only operate on capped collections.")
		
		# We track the last seen ID to allow us to efficiently re-query from where we left off.
		last = None
		
		while self._running:
			cursor = collection.find(query, tailable=True, await_data=True, **q._cursor_args)
			
			while self._running:
				try:
					record = next(cursor)
				except StopIteration:
					if not cursor.alive:
						break
					
					record = None
				
				if record is not None:
					yield self._document._from_son(record, _auto_dereference=self._auto_dereference)
					last = record['_id']
				
				if timeout and time() >= end:
					return
			
			if last:
				query.update(_id={"$gt": last})


class TaskQuerySet(QuerySet):
	"""A custom queryset bundling common Task queries."""
	
	def incomplete(self, *q_objs, **query):
		"""Search for tasks that aren't yet completed.
		
		Matched states: pending, accepted, running
		"""
		
		return self.clone().filter(time__completed=None, time__cancelled=None).filter(*q_objs, **query)
	
	def pending(self, *q_objs, **query):
		"""Search for tasks that are pending."""
		
		# If it's never been acquired, it can't be running or complete.
		return self.clone().filter(time__acquired=None, time__cancelled=None).filter(*q_objs, **query)
	
	def accepted(self, *q_objs, **query):
		"""Search for tasks that have been accepted for work, but aren't yet running."""
		
		return self.clone().filter(time__acquired__ne=None, time__executed=None, time__cancelled=None).filter(*q_objs, **query)
	
	def running(self, *q_objs, **query):
		"""Search for tasks that are actively running."""
		
		return self.clone().filter(time__executed__ne=None, time__completed=None, time__cancelled=None).filter(*q_objs, **query)
	
	def failed(self, *q_objs, **query):
		"""Search for tasks that have failed."""
		
		return self.clone().filter(exception__ne=None).filter(*q_objs, **query)
	
	def finished(self, *q_objs, **query):
		"""Search for tasks that have finished, successfully or not."""
		return self.clone().filter(Q(time__cancelled__ne=None) | Q(time__completed__ne=None)).filter(*q_objs, **query)
	
	def complete(self, *q_objs, **query):
		"""Search for tasks that completed successfully."""
		
		return self.clone().finished(time__cancelled=None, task_exception=None).filter(*q_objs, **query)
	
	def cancelled(self, *q_objs, **query):
		"""Search for tasks that were explicitly cancelled."""
		
		return self.clone().filter(time__cancelled__ne=None).filter(*q_objs, **query)
