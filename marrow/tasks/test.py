def producer():
    from pymongo import MongoClient
    from queue import setup, queue

    conn = MongoClient()
    db = conn.test

    queue = setup(db, 'messages', 16)

    try:
        while True:
            queue.insert({'a':1}, safe=False)
    except KeyboardInterrupt:
        queue.insert({'stop': True}, safe=False)


def consumer():
    from pymongo import MongoClient
    from queue import setup, queue

    conn = MongoClient()
    db = conn.test

    queue = queue(db, 'messages', size=16)

    rec = None
    while rec is None or rec.get('nop'):
        rec = next(queue)

    from time import time

    n = 0
    start = time()

    for rec in queue:
        n += 1

    duration = time() - start
    timeper = duration / float(n) * 1000
    msgper = float(n) / duration

    print "%0.2fs for %d messages: %0.2f usec/gen (%d messages/sec)" % (duration, n, timeper, msgper)
