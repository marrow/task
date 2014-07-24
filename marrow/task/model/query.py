# encoding: utf-8

from __future__ import unicode_literals

from mongoengine import QuerySet
from pymongo.errors import OperationFailure, ExecutionTimeout


class CappedQuerySet(QuerySet):
	"""A cusom queryset that allows for tailing of capped collections."""
	
	def tail(self, *q_objs, **query):
		"""A generator which will block and yield entries as they are added to the collection.
		
		Only use this on capped collections; ones with meta containing `max_size` and/or `max_documents`.
		
		Accepts the int/float `timeout` named argument indicating a number of seconds to wait for a result.
		
		(Yes, this means you can't query a field called `timeout` here.  Call .filter manually if needed.)
		
		for obj in MyDocument.objects.tail():
			print(obj)
		
		Additional important note: tailing will fail (badly) if the collection is empty.  Always prime the collection
		with an empty or otherwise unimportant record before attempting to use this feature.
		
		TODO: Master/slave data source selection.
		"""
		
		# Process the timeout value, if one is provided.
		timeout = query.pop('timeout', None)
		if timeout: timeout = int(timeout * 1000)
		
		# Prepare the query and extract often-reused values.
		q = self.clone().filter(*q_objs, **query)
		collection = q._collection
		query = q._query
		
		if not collection.options().get('capped', False):
			raise TypeError("Can only operate on capped collections.")
		
		# We track the last seen ID to allow us to efficiently re-query from where we left off.
		last = None
		
		try:
			while True:  # Primary retry loop.
				cursor = collection.find(query, tailable=True, await_data=True)
				if timeout: cursor = cursor.max_time_ms(timeout)
				
				while cursor.alive:  # Inner record loop; may time out.
					for record in cursor:  # This will block until data is available.
						last = record['_id']
						yield self._document._from_son(record, _auto_dereference=self._auto_dereference)
				
				query.update(_id={"$gte": last})
		
		except ExecutionTimeout:
			return


class TaskQuerySet(QuerySet):
	"""A cusom queryset bundling common Task queries."""
	
	def incomplete(self, *q_objs, **query):
		"""Search for tasks that aren't yet completed.
		
		Matched states: pending, accepted, running
		"""
		
		return self.clone().filter(completed=None, cancelled=None).filter(*q_objs, **query)
	
	def pending(self, *q_objs, **query):
		"""Search for tasks that are pending."""
		
		# If it's never been acquired, it can't be running or complete.
		return self.clone().filter(acquired=None, cancelled=None).filter(*q_objs, **query)
	
	def accepted(self, *q_objs, **query):
		"""Search for tasks that have been accepted for work, but aren't yet running."""
		
		return self.clone().filter(acquired__ne=None, executed=None, cancelled=None).filter(*q_objs, **query)
	
	def running(self, *q_objs, **query):
		"""Search for tasks that are actively running."""
		
		return self.clone().filter(executed__ne=None, completed=None, cancelled=None).filter(*q_objs, **query)
	
	def failed(self, *q_objs, **query):
		"""Search for tasks that have failed."""
		
		return self.clone().filter(exception__ne=None).filter(*q_objs, **query)
	
	def complete(self, *q_objs, **query):
		"""Search for tasks that completed successfully."""
		
		return self.clone().filter(completed__ne=None, exception=None).filter(*q_objs, **query)
	
	def cancelled(self, *q_objs, **query):
		"""Search for tasks that were explicitly cancelled."""
		
		return self.clone().filter(cancelled__ne=None).filter(*q_objs, **query)
