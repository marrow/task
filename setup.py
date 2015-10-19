#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import os
import sys
import codecs


try:
	from setuptools.core import setup, find_packages
except ImportError:
	from setuptools import setup, find_packages

from setuptools.command.test import test as TestCommand


if sys.version_info < (2, 6):
	raise SystemExit("Python 2.6 or later is required.")
elif sys.version_info > (3, 0) and sys.version_info < (3, 2):
	raise SystemExit("Python 3.2 or later is required.")

exec(open(os.path.join("marrow", "task", "release.py")).read())


class PyTest(TestCommand):
	def finalize_options(self):
		TestCommand.finalize_options(self)
		
		self.test_args = []
		self.test_suite = True
	
	def run_tests(self):
		import pytest
		sys.exit(pytest.main(self.test_args))


here = os.path.abspath(os.path.dirname(__file__))

tests_require = ['pytest', 'pytest-cov', 'pytest-spec', 'pytest_cagoule', 'pytest-flakes']

install_requires = [
		'apscheduler', 'pymongo==2.8', 'mongoengine==0.9.0', 'pytz', 'marrow.package<2.0', 'wrapt<2.0', 'pyyaml'
	] + (['futures'] if sys.version_info < (3,) else []) + (['ordereddict'] if sys.version_info < (2, 7) else [])

setup(
	name = "marrow.task",
	version = version,
	
	description = description,
	long_description = codecs.open(os.path.join(here, 'README.rst'), 'r', 'utf8').read(),
	url = url,
	
	author = author.name,
	author_email = author.email,
	
	license = 'MIT',
	keywords = '',
	classifiers = [
			"Development Status :: 5 - Production/Stable",
			"Environment :: Console",
			"Intended Audience :: Developers",
			"License :: OSI Approved :: MIT License",
			"Operating System :: OS Independent",
			"Programming Language :: Python",
			"Programming Language :: Python :: 2.6",
			"Programming Language :: Python :: 2.7",
			"Programming Language :: Python :: 3",
			"Programming Language :: Python :: 3.2",
			"Programming Language :: Python :: 3.3",
			"Programming Language :: Python :: 3.4",
			"Programming Language :: Python :: Implementation :: CPython",
			"Programming Language :: Python :: Implementation :: PyPy",
			"Topic :: Software Development :: Libraries :: Python Modules"
		],
	
	packages = find_packages(exclude=['test', 'script', 'example']),
	include_package_data = True,
	namespace_packages = ['marrow'],
	
	install_requires = install_requires,
	
	extras_require = dict(
			development = tests_require,
		),
	
	tests_require = tests_require,
	
	entry_points = {
			'futures.executor': [
					'thread = concurrent.futures:ThreadPoolExecutor',
					'process = concurrent.futures:ProcessPoolExecutor',
				],
			'console_scripts': [
					'marrow.task = marrow.task.runner:default_runner',
					'marrow.task.test = marrow.task.runner:test_func',
				]
		},

	zip_safe = False,
	cmdclass = dict(
			test = PyTest,
		),
)
