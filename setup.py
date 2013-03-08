#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import warnings

from setuptools import setup, find_packages


if sys.version_info < (2, 6):
	raise SystemExit("Python 2.6 or later is required.")

if sys.version_info > (3, 0):
	warnings.warn("Marrow Tasks is untested on Python 3, but should in theory work.", RuntimeWarning)

exec(open(os.path.join("marrow", "tasks", "release.py")).read())



setup(
		name = "marrow.tasks",
		version = version,

		description = "A high-performance contender for asynchronous task execution.",
		long_description = """\
For full documentation, see the README.textile file present in the package,
or view it online on the GitHub project page:

https://github.com/marrow/marrow.tasks""",

		author = "Alice Bevan-McGregor",
		author_email = "alice+marrow@gothcandy.com",
		url = "https://github.com/marrow/marrow.tasks",
		license = "MIT",

		install_requires = [
			'marrow.util < 2.0',
			'pymongo >= 2.4.2',
			'dateutil'
		] + [
			'futures'
		] if sys.version_info < (3, 0) else [],

		test_suite = 'nose.collector',
		tests_require = [
			'nose',
			'coverage',
			'transaction',
		] + [
			'futures'
		] if sys.version_info < (3, 0) else [],

		classifiers=[
			"Development Status :: 5 - Production/Stable",
			"Environment :: Console",
			"Intended Audience :: Developers",
			"License :: OSI Approved :: MIT License",
			"Operating System :: OS Independent",
			"Programming Language :: Python",
			"Programming Language :: Python :: 2.6",
			"Programming Language :: Python :: 2.7",
			# "Programming Language :: Python :: 3",
			# "Programming Language :: Python :: 3.1",
			# "Programming Language :: Python :: 3.2",
			"Topic :: Software Development :: Libraries :: Python Modules"
		],

		packages = find_packages(exclude=['examples', 'tests']),
		zip_safe = True,
		include_package_data = True,
		package_data = {'': ['README.textile', 'LICENSE']},

		namespace_packages = ['marrow'],

		entry_points = {
			# 'marrow.mailer.manager': [
			# 	'immediate = marrow.mailer.manager.immediate:ImmediateManager',
			# 	'futures = marrow.mailer.manager.futures:FuturesManager',
			# 	'dynamic = marrow.mailer.manager.dynamic:DynamicManager',
			# 	# 'transactional = marrow.mailer.manager.transactional:TransactionalDynamicManager'
			# ],
		}
	)