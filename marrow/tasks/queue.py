# encoding: utf-8

"""MongoDB utility functions for consuming a capped collection as a queue."""

from pymongo.errors import OperationFailure

#from marrow.tasks.exc import NotCappedException

class NotCappedException(Exception):
	pass


base_worker_query = {'result': {'$exists': False}}


def setup(db, name, size=None, log=None):
    queue = db[name]

    if not queue.options().get('capped', False):
        if not size:
            raise NotCappedException("Collection is not capped; specify a size to correct this.")
        
        if log:
            log.warn("Creating capped collection \"" + collection_name + "\" " + str(size) + " MiB in size.")
        db.drop_collection(name)
        db.create_collection(name, size=size * 1024 * 1024, capped=True)
        queue = db[name]
    
    if not queue.count():
        # This is to prevent a terrible infinite busy loop while empty.
        queue.insert(dict(nop=True))

    return queue


def queue(db, name, query=None, size=None, identity=None, log=None):
    queue = setup(db, name, size, log)
    last = None
    query = query or {}

    # Primary retry loop.
    # If the cursor dies, we need to be able to restart it.
    while True:
        cursor = queue.find(query, sort=[('$natural', 1)], slave_ok=True, tailable=True, await_data=True)
        
        try:
            # Inner record loop.
            # We may reach the end of the for loop (timeout waiting for
            # new records) at which point we should re-try the for loop as
            # long as the cursor is still alive.  If it isn't, re-query.
            while cursor.alive:
                for record in cursor:
                    # Update the last record ID for later re-querying.
                    last = record['_id']
                    
                    # We can be asked to stop.
                    if record.get('stop', False):
                        who = record.get('target')
                        if not who or (identity and who == identity):
                            if log:
                                log.debug("Queue exiting due to explicit shutdown message.")
                            return

                    yield record

        except OperationFailure:
            pass
        
        if log:
            log.debug("Continuing from: " + str(last) + "(" + last.generation_time.isoformat(' ') + ")")
        
        # Update the query to continue from where we left off.
        query = ({"_id": {"$gte": last}}).update(query)
