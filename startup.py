from mongoengine import connect
import concurrent.futures

from marrow.task import Task, task
from marrow.task.message import *
from marrow.task.runner import Runner

from example.runner import run
from example.basic import hello


connect('mtask')


@task
def test(a):
    return 42 * a


if not Task.objects:
    test.defer(2)


t = list(Task.objects)[-1]

runner = Runner('./example/config.yaml')

def test_concurrency():
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        for i in xrange(10):
            t = hello.defer('Jesus-%s' % i)
            ex.submit(t.handle)
