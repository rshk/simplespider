"""
Tests for the main Spider object
"""

import pytest

from simplespider import Spider, BaseTask, BaseTaskRunner, \
    AbortTask, SkipRunner, RetryTask


class MyTask(BaseTask):
    pass


class MyOtherTask(BaseTask):
    pass


@pytest.fixture
def spider():

    tasks_execution_log = []

    class LoggingTaskRunner(BaseTaskRunner):
        def __call__(self, task):
            tasks_execution_log.append((self, task.id))
            if task.get('abortme', False):
                raise AbortTask()
            if task.get('skipme', False):
                raise SkipRunner()
            if task.get('retryme', False):
                raise RetryTask()
            if task.get('raise_sth', False):
                raise Exception("This is an exception!")
            return iter([])

    class MyTaskRunner(LoggingTaskRunner):
        def match(self, task):
            return isinstance(task, MyTask)

    class MyTaskRunner2(LoggingTaskRunner):
        def match(self, task):
            return isinstance(task, MyTask)

        def __call__(self, task):
            super(MyTaskRunner2, self).__call__(task)
            yield MyOtherTask(
                task_id='was:' + task.id,
                previous=task)

    class MyOtherTaskRunner(LoggingTaskRunner):
        def match(self, task):
            return isinstance(task, MyOtherTask)

    runners = {
        0: MyTaskRunner(),
        1: MyTaskRunner2(),
        2: MyOtherTaskRunner(),
    }

    spider = Spider()
    spider.add_runners(x[1] for x in sorted(runners.iteritems()))
    # spider.execution_log = tasks_execution_log

    ## We need to pass some extra stuff..
    spider._testing = {
        'execution_log': tasks_execution_log,
        'runners': runners,
    }

    return spider


def test_task_dispatching(spider):
    execution_log = spider._testing['execution_log']
    runners = spider._testing['runners']

    ## This will be run twice and generate an extra task
    spider.queue_task(MyTask('task-1'))

    ## This will be run once
    spider.queue_task(MyOtherTask('task-2'))

    ## This will be run only once, as the first runner
    ## will ask for task abortion.
    spider.queue_task(MyTask('task-3', abortme=True))

    ## This will cause early skip, so no new task will
    ## be scheduled. A part from that, this task will
    ## be run twice as usual.
    spider.queue_task(MyTask('task-4', skipme=True))

    ## Test deduplication
    spider.queue_task(MyTask('task-5'))
    spider.queue_task(MyTask('task-5'))
    spider.queue_task(MyTask('task-5'))

    ## Test retrying
    spider.queue_task(MyTask('task-6', retryme=True, retry=2))

    ## Test retrying on generic exception
    spider.queue_task(MyTask('task-7', raise_sth=True, retry=2))

    spider.run()

    #assert len(execution_log) == 16  # count lines below :)

    assert execution_log.pop(0) == (runners[0], 'task-1')
    assert execution_log.pop(0) == (runners[1], 'task-1')
    assert execution_log.pop(0) == (runners[2], 'task-2')
    assert execution_log.pop(0) == (runners[0], 'task-3')
    assert execution_log.pop(0) == (runners[0], 'task-4')
    assert execution_log.pop(0) == (runners[1], 'task-4')
    assert execution_log.pop(0) == (runners[0], 'task-5')
    assert execution_log.pop(0) == (runners[1], 'task-5')

    assert execution_log.pop(0) == (runners[0], 'task-6')
    assert execution_log.pop(0) == (runners[0], 'task-7')

    assert execution_log.pop(0) == (runners[2], 'was:task-1')
    assert execution_log.pop(0) == (runners[2], 'was:task-5')

    assert execution_log.pop(0) == (runners[0], 'task-6[R]')
    assert execution_log.pop(0) == (runners[0], 'task-7[R]')
    assert execution_log.pop(0) == (runners[0], 'task-6[R][R]')
    assert execution_log.pop(0) == (runners[0], 'task-7[R][R]')

    assert len(execution_log) == 0  # we popped 'em all

    with pytest.raises(TypeError):
        spider.run_task('this is not a task')
    with pytest.raises(TypeError):
        spider.queue_task('this is not a task')
