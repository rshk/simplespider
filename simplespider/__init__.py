"""
Simple spider.

Keeps & manages a collection of downloaders and scrapers.

Downloaders will:

* Receive a URL to download
* Download it and yield crawling tasks

Crawlers will:

* Receive data from the crawling task
* Yield objects found in the page
"""

from collections import defaultdict
import anydbm
import copy
import json
import logging
import sys
import uuid

import six


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
try:
    from cool_logging.formatters import ConsoleColorFormatter
    # pip install cool_logging==0.2-beta
except ImportError:
    handler.setFormatter(logging.Formatter(
        "%(levelname)s %(filename)s:%(lineno)d %(funcName)s: %(message)s"))
else:
    handler.setFormatter(ConsoleColorFormatter())
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class BaseTask(object):
    __slots__ = ['_id', '_type', '_attributes']

    def __init__(self, task_id, **kwargs):
        """
        Base for the spider tasks.
        Tasks need to be hashable and immutable, so we can be sure,
        for example, that we don't run the same task twice.
        Also, we don't want its attributes to be changed at runtime.

        :param retry:
            Number of times this task should be retried upon failure.
            Defaults to 2 (for a total of 3 exectutions).
        """
        if not isinstance(task_id, six.string_types):
            raise TypeError("task_id must be a string!")
        if isinstance(task_id, six.text_type):
            ## We want binary strings as task ids
            task_id = task_id.encode('utf-8')
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
            self._type = '.'.join((self.__class__.__module__,
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
        return "{0}({1}, {2})".format(
            self.type,
            self.id,
            ', '.join('{0}={1!r}'.format(k, v)
                      for k, v in sorted(tuple(self._attributes.iteritems()))))

    def to_dict(self):
        attrs = copy.deepcopy(self._attributes)
        attrs['_id'] = self.id
        attrs['_type'] = self.type
        return attrs

    # def __getstate__(self):
    #     return dict(**self)

    # def __setstate__(self, state):
    #     dict.update(self, state)


class BaseTaskRunner(object):
    def __init__(self, **kwargs):
        self.conf = kwargs

    def match(self, task):
        pass

    def __call__(self, task):
        pass


class BaseObject(dict):
    """Base for the objects retrieved by the scraper"""

    __slots__ = []  # We don't want attributes

    def __repr__(self):
        return "{0}({1})".format(
            self.type,
            ', '.join('{0}={1!r}'.format(name, value)
                      for name, value in sorted(self.iteritems())))

    @property
    def type(self):
        return '.'.join((self.__class__.__module__, self.__class__.__name__))

    def __getstate__(self):
        """When pickling, we don't care about attributes"""
        return tuple(self.iteritems())

    def __setstate__(self, state):
        self.update(state)


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
        self._task_queue.push(task.id, task)

    # def pop_task(self):
    #     """Get next task from the queue"""
    #     return self._task_queue.pop()

    def yield_tasks(self):
        """Continue yielding tasks until queue is empty"""
        while True:
            try:
                logger.debug("Task queue length is {0}".format(
                    len(self._task_queue)))
                task = self._task_queue.pop()
                if task is None:
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
                logger.debugger("Runner asked to be skipped")
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
                    new_task['retry'] = task['retry'] - 1
                    ## todo: we need to change the id, or the task will
                    ##       never be executed again!
                    self.queue_task(new_task)
                else:
                    logger.info("Max retries reached. Aborting task {0!r}.")

        logger.info("Task execution successful")

    def _wrap_task_execution(self, runner, task):
        logger.info("Starting task: {0!r} (via {1!r})".format(task, runner))
        for item in runner(task):
            if isinstance(item, BaseTask):
                logger.debug("  -> Got new task {0!r}".format(item))
                self.queue_task(item)

            elif isinstance(item, BaseObject):
                logger.debug("  -> Got new object: {0!r}".format(item))
                self._store(item)

            else:
                logger.warning("  -> I don't know what to do with: {0!r}"
                               "".format(item))

    def _store(self, obj):
        logger.debug("Storing object: {0!r}".format(obj))
        if self._storage is not None:
            self._storage.save(obj)

    @property
    def _storage(self):
        if not self.conf.get('storage'):
            klass = self.conf.get('storage_manager', DictStorage)
            self.conf['storage'] = klass()
        return self.conf['storage']

    @property
    def _task_queue(self):
        if not self.conf.get('queue'):
            klass = self.conf.get('queue_manager', ListQueueManager)
            self.conf['queue'] = klass()
        return self.conf['queue']


class BaseStorage(object):
    def save(self, obj):
        raise NotImplementedError

    def sync(self, obj):
        pass


class DictStorage(BaseStorage):
    def __init__(self):
        self._storage = defaultdict(list)

    def save(self, obj):
        self._storage[type(obj).__name__].append(obj)


class AnydbmStorage(BaseStorage):
    def __init__(self, path):
        self._storage_path = path
        self._storage = anydbm.open(path, 'c')

    def save(self, obj):
        obj._type = type(obj).__name__
        obj._id = getattr(obj, 'id', None) or str(uuid.uuid4())
        storage_key = "{0}.{1}".format(obj._type, obj._id)
        self._storage[storage_key] = json.dumps(obj.export())

    def sync(self):
        self._storage.sync()


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
