===========
Marrow Task
===========

    © 2013-2014 Alice Bevan-McGregor and contributors.

..

    https://github.com/marrow/marrow.task

..

    |latestversion| |downloads| |masterstatus| |mastercover| |issuecount|

1. What is Marrow Task?
=======================

Marrow Task is a light-weight yet powerful, fully tested, Python 2.7+ and 3.3+ compatible distributed task execution
system.  It is built entirely around several core (though under-utilized) features of MongoDB and builds upon the
powerful MongoEngine object document mapper.

It is similar to Celery or other distributed task systems excluding the fact that if your application already uses
MongoDB, Marrow Task can be far easier to set up and integrate.  It is substantially simpler to configure, and supports
several preculiar features out of the box:

* Deferred execution of generators.  Write code that yields data on one host, and consumes the data as it is yielded on
  another.

* Track task progress in realtime.

* Gather statistics on task execution automatically.

* Avoid data serialization formats (i.e. pickle) by using only native BSON types.

* The ability to execute any function that can be imported, including classmethods and staticmethods of classes.

* The ability to subclass the Task type and execute methods on instances of MongoEngine database objects.

* Being Futures-compatible you can use Marrow Task as a drop-in replacement for a more naive thread or process pool.

* Explicitly does not add any new infrastructure dependencies to your own application: no need for an additional queue
  like Zeromq or Rabbitmq, or storage system like Redis, and the accompanying service monitoring or even extra hardware
  required by such.


1.1 Goals
---------

Marrow Task was created to be a light-weight alternative to systems like Celery that, as noted above, explicitly does
not add to the infrastructure maintenance burden of systems administrators.  Additionally, it was intended to fill
several gaps in functionality in competing systems, often excluded for performance reasons, such as task timeline
analytics, while providing an interface compatible with Futures, a universally standardized API for deferred execution.

While Celery didn't exactly fit some of our own requirements, Marrow Task might not fit yours.  Always plan out your
application's requirements before deciding on the right tools for the job.


2. Installation
===============

Installing ``marrow.task`` is easy, just execute the following in a terminal::

    pip install marrow.task

If you add ``marrow.task`` to the ``install_requires`` argument of the call to ``setup()`` in your applicaiton's
``setup.py`` file, Marrow Task will be automatically installed and made available when your own application or
library is installed.  We recommend using "less than" version numbers to ensure there are no unintentional
side-effects when updating.  Use ``marrow.task<1.1`` to get all bugfixes for the current release, and
``marrow.task<2.0`` to get bugfixes and feature updates while ensuring that large breaking changes are not installed.


2.1. Requirements
-----------------

Due to some of the fairly advanced things Marrow Task does, certain versions of Python are explicitly not supported.
Notably due to the removal of function wrappers (which add ``im_class`` to the mix) in Python 3.0, only Python 3
versions later than 3.3 (which introduced ``__qualname__`` to replace ``im_class`` magic) will work if you make use of
static or class methods as execution targets.

This restriction is soft; if you choose to install Marrow Task on earlier Python 3 versions we will issue a huge
warning up front, but the remainder of the functionality should continue to operate.

Additionally, several other components are needed in order to make use of Marrow Task:

* A MongoDB 2.0 or later service.
* MongoEngine


2.2. Development Version
------------------------

    |developstatus| |developcover|

Development takes place on `GitHub <https://github.com/>`_ in the
`marrow.task <https://github.com/marrow/marrow.task/>`_ project.  Issue tracking, documentation, and downloads
are provided there.

Installing the current development version requires `Git <http://git-scm.com/>`_, a distributed source code management
system.  If you have Git you can run the following to download and *link* the development version into your Python
runtime::

    git clone https://github.com/marrow/marrow.task.git
    (cd marrow.task; python setup.py develop)

You can then upgrade to the latest version at any time::

    (cd marrow.task; git pull; python setup.py develop)

If you would like to make changes and contribute them back to the project, fork the GitHub project, make your changes,
and submit a pull request.  This process is beyond the scope of this documentation; for more information see
`GitHub's documentation <http://help.github.com/>`_.


3. Basic Concepts
=================

TBD


4. Version History
==================

Version 1.0
-----------

* Initial release.


5. License
==========

Marrow Task has been released under the MIT Open Source license.

5.1. The MIT License
--------------------

Copyright © 2013-2014 Alice Bevan-McGregor and contributors.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the “Software”), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


.. |masterstatus| image:: http://img.shields.io/travis/marrow/task/master.svg?style=flat
    :target: https://travis-ci.org/marrow/marrow.task
    :alt: Release Build Status

.. |developstatus| image:: http://img.shields.io/travis/marrow/task/develop.svg?style=flat
    :target: https://travis-ci.org/marrow/marrow.task
    :alt: Development Build Status

.. |latestversion| image:: http://img.shields.io/pypi/v/marrow.task.svg?style=flat
    :target: https://pypi.python.org/pypi/marrow.task
    :alt: Latest Version

.. |downloads| image:: http://img.shields.io/pypi/dw/marrow.task.svg?style=flat
    :target: https://pypi.python.org/pypi/marrow.task
    :alt: Downloads per Week

.. |mastercover| image:: http://img.shields.io/coveralls/marrow/task/master.svg?style=flat
    :target: https://travis-ci.org/marrow/marrow.task
    :alt: Release Test Coverage

.. |developcover| image:: http://img.shields.io/coveralls/marrow/task/develop.svg?style=flat
    :target: https://travis-ci.org/marrow/marrow.task
    :alt: Development Test Coverage

.. |issuecount| image:: http://img.shields.io/github/issues/marrow/task.svg?style=flat
    :target: https://github.com/marrow/marrow.task/issues
    :alt: Github Issues

.. |cake| image:: http://img.shields.io/badge/cake-lie-1b87fb.svg?style=flat
