# encoding: utf-8

"""Exception classes."""


class TaskException(Exception):
	pass


class DataException(Exception):
	pass


class NotCappedException(DataException):
	pass
	