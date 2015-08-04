# encoding: utf-8

from marrow.task.runner import Runner


def run():
	runner = Runner('config.yaml')
	runner.run()


if __name__ == '__main__':
	run()
