# encoding: utf-8

class LocalTask(object):
	def __init__(self, wrapped, args, kwargs):
		# print('Wrapped: ', wrapped, 'Args: ', args, 'Kwargs: ', kwargs)
		self.result = self.exception = None
		try:
			self.result = wrapped(*args, **kwargs)
		except Exception as exception:
			self.exception = exception
		self.state = 'complete' if self.exception is None else 'failed'