"""
Simple spider core.

The spider class will simply handle a queue of tasks
and distribute them amongst a bunch of runners.
"""

import copy
import logging
import sys
import uuid

import six

__version__ = '0.1a'


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
try:
    from cool_logging.formatters import ConsoleColorFormatter
    # pip install cool_logging==0.2-beta
except ImportError:  # pragma: no cover
    handler.setFormatter(logging.Formatter(
        "%(levelname)s %(filename)s:%(lineno)d %(funcName)s: %(message)s"))
else:
    handler.setFormatter(ConsoleColorFormatter())
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class BaseTask(object):
    __slots__ = ['_id', '_type', '_attributes']

    def __init__(self, task_id=None, **kwargs):
        """
        Base for the spider tasks.
        Tasks need to be hashable and immutable, so we can be sure,
        for example, that we don't run the same task twice.
        Also, we don't want its attributes to be changed at runtime.

        :param retry:
            Number of times this task should be retried upon failure.
            Defaults to 2 (for a total of 3 exectutions).
        """
        if task_id is None:
            task_id = str(uuid.uuid4())

        if not isinstance(task_id, six.string_types):
            raise TypeError("task_id must be a string!")

        self._id = task_id

        ## We want to make sure we don't have references
        ## to other objects that might be changed..
        kwargs = copy.deepcopy(kwargs)

        kwargs.setdefault("retry", 2)

        self._attributes = kwargs

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        if getattr(self, '_type', None) is None:
            self._type = ':'.join((self.__class__.__module__,
                                   self.__class__.__name__))
        return self._type

    def __getitem__(self, name):
        return self._attributes[name]

    def __setitem__(self, name, value):
        self._attributes[name] = value

    def __delitem__(self, name):
        del self._attributes[name]

    def get(self, *a, **kw):
        return self._attributes.get(*a, **kw)

    def update(self, *a, **kw):
        return self._attributes.update(*a, **kw)

    def __repr__(self):
        return "{0}({1!r}, {2})".format(
            self.type,
            self.id,
            ', '.join('{0}={1!r}'.format(k, v)
                      for k, v in sorted(tuple(self._attributes.iteritems()))))

    @classmethod
    def from_dict(cls, data):
        data = copy.deepcopy(data)  # so we can safely modify..
        if '_type' in data:
            module, name = data.pop('_type').split(':')
            mod = __import__(module, globals(), globals(), [name])
            klass = getattr(mod, name)
            if not isinstance(klass, BaseTask):
                raise TypeError("Invalid object: not a BaseTask")
        else:
            klass = cls
        task_id = data.pop('_id', None)
        return klass(task_id, **data)

    def to_dict(self):
        attrs = copy.deepcopy(self._attributes)
        attrs['_id'] = self.id
        attrs['_type'] = self.type
        return attrs

    def __getstate__(self):
        """Before pickling"""
        return dict(id=self._id, attrs=self._attributes)

    def __setstate__(self, state):
        """After pickling"""
        self._id = state['id']
        self._attributes = state['attrs']

    def __eq__(self, other):
        """Comparison is mostly needed for tests"""
        return ((self.id == other.id)
                and (self._attributes == other._attributes)
                and (self.type == other.type))


class BaseTaskRunner(object):
    def __init__(self, **kwargs):
        self.conf = kwargs

    def match(self, task):
        return True  # So we can safely use super() on this..

    def __call__(self, task):
        return  # So we can safely use super() on this..


class RetryTask(Exception):
    """Ask for the task to be retried"""
    pass


class AbortTask(Exception):
    """Ask for the current task not to be retrieved again"""
    pass


class SkipRunner(Exception):
    """Exception used to tell to "skip" the current task runner"""
    pass


class Spider(object):
    def __init__(self, **kwargs):
        """
        All the passed keyword arguments gets aggregated in the
        ``conf`` attribute (dict).

        :param storage: object used to store objects
        :param queue: object used to handle the task queue
        """

        self.conf = kwargs

        ## Registers of downloaders and scrapers
        self._runners = []

        ## Keep a list of already downloaded
        ## URLs to prevent infinite recursion.
        ## todo: we need a smarter way to do this..
        self._already_done = set()

    def add_runners(self, runners):
        self._runners.extend(runners)

    def _get_runners(self, task):
        logger.debug("Looking for runners suitable to run {0!r}".format(task))
        logger.debug("We have {0} registered runners".format(
            len(self._runners)))
        for runner in self._runners:
            logger.debug("Trying {0!r}".format(runner))
            if runner.match(task):
                logger.debug("Runner {0!r} matched".format(runner))
                yield runner

    def queue_task(self, task):
        """Queue a task for later execution"""
        logger.debug("Scheduling new task: {0!r}".format(task))
        if not isinstance(task, BaseTask):
            raise TypeError("This doesn't look like a task!")
        self._task_queue.push(task.id, task)

    def yield_tasks(self):
        """Continue yielding tasks until queue is empty"""
        while True:
            try:
                logger.debug("Task queue length is {0}".format(
                    len(self._task_queue)))
                task = self._task_queue.pop()
                if task is None:  # pragma: no cover
                    ## This in the rare case of a misbehaving queue..
                    raise IndexError("Null task received")
            except IndexError:  # queue empty
                logger.info("Queue empty. Terminating execution.")
                return
            else:
                yield task

    def run(self):
        """Start execution of the queue, until no tasks are left"""
        for name, task in self.yield_tasks():
            self.run_task(task)

    def run_task(self, task):
        """Run a given task"""

        logger.info("Starting task execution: {0!r}".format(task))

        if not isinstance(task, BaseTask):
            raise TypeError("This doesn't look like a task!")

        for runner in self._get_runners(task):
            logger.debug("Starting execution with {0!r}".format(runner))

            ## todo: we need to stop if asked to do so, etc.
            try:
                self._wrap_task_execution(runner, task)

            except AbortTask:
                logger.info("Task {0!r} aborted".format(task))
                return  # And never execute this anymore!

            except SkipRunner:
                logger.debug("Runner asked to be skipped")
                pass  # Ok, let's just skip this..

            except Exception, e:
                if not isinstance(e, RetryTask):
                    ## We retry failing tasks, but we notify the user, if the
                    ## exception wasn't a RetryTask
                    logger.warning("Task failed with unknown exception. "
                                   "Re-scheduling it for retry.")
                    logger.exception("")
                if task['retry'] > 0:
                    logger.info("Task {0!r} to be retried "
                                "{1} more times".format(task, task['retry']))
                    new_task = copy.deepcopy(task)
                    new_task._id += '[R]'  # to make sure this is run again
                    new_task['retry'] = task['retry'] - 1
                    ## todo: we need to change the id, or the task will
                    ##       never be executed again!
                    self.queue_task(new_task)
                else:
                    logger.info("Max retries reached. Aborting task {0!r}.")
                return  # do not continue with other runners..

        logger.info("Task execution successful")

    def _wrap_task_execution(self, runner, task):
        logger.info("Starting task: {0!r} (via {1!r})".format(task, runner))
        for item in runner(task):
            if isinstance(item, BaseTask):
                logger.debug("  -> Got new task {0!r}".format(item))
                self.queue_task(item)

            else:
                logger.warning("  -> I don't know what to do with: {0!r}"
                               "".format(item))

    @property
    def _task_queue(self):
        if not self.conf.get('queue'):
            klass = self.conf.get('queue_manager', ListQueueManager)
            self.conf['queue'] = klass()
        return self.conf['queue']


class BaseQueueManager(object):
    def pop(self):
        """Pops a task from the queue"""
        raise NotImplementedError

    def push(self, name, task):
        """Pushes a task to the queue"""
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError


class ListQueueManager(object):
    """
    Simple queue manager, using a list to keep track
    of the tasks.

    .. warning::
        This will quickly grow out of memory for large sites!
    """

    def __init__(self):
        self._queue = []
        self._dedup_set = set()

    def pop(self):
        return self._queue.pop(0)

    def push(self, name, task):
        if name in self._dedup_set:
            logger.debug("Task {0!r} was already executed. "
                         "Not queuing.".format(name))
            return
        self._dedup_set.add(name)
        self._queue.append((name, task))

    def __len__(self):
        return len(self._queue)
