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

from collections import defaultdict, namedtuple
from functools import wraps
import anydbm
import copy
import json
import logging
import re
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


class BaseTask(dict):
    __slots__ = []

    def __init__(self, **kwargs):
        """
        Base for the spider tasks.
        Tasks need to be hashable and immutable, so we can be sure,
        for example, that we don't run the same task twice.
        Also, we don't want its attributes to be changed at runtime.

        :param retry:
            Number of times this task should be retried upon failure.
            Defaults to 2 (for a total of 3 exectutions).
        """
        kwargs.setdefault("retry", 2)
        super(BaseTask, self).update(copy.deepcopy(kwargs))

        ## We call hash() here to trigger an exception ASAP
        ## if some value is not hashable..
        hash(self)

    def __readonly_error(self):
        raise TypeError("{0}.{1} object is readonly".format(
            self.__class__.__module__,
            self.__class__.__name__))

    def __setitem__(self, name, value):
        self.__readonly_error()

    def __delitem__(self, name):
        self.__readonly_error()

    def update(self, *a, **kw):
        self.__readonly_error()

    def __copy__(self):
        return self.__class__(**self)

    def __deepcopy__(self, memo):
        return self.__class__(**self)

    def clone(self, **kwargs):
        new_kwargs = dict(**self)
        new_kwargs.update(kwargs)
        return self.__class__(**new_kwargs)

    def __hash__(self):
        return hash(tuple(self.iteritems()))

    def __eq__(self, other):
        #raise Exception("HERE")
        return dict(**self) == dict(**other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "{0}.{1}({2})".format(
            self.__class__.__module__,
            self.__class__.__name__,
            ', '.join('{0}={1!r}'.format(k, v)
                      for k, v in sorted(tuple(self.iteritems()))))

    def __getstate__(self):
        return dict(**self)

    def __setstate__(self, state):
        dict.update(self, state)


class DownloadTask(BaseTask):
    __slots__ = []

    def __init__(self, **kwargs):
        """
        A downloading task.

        :param url: the URL to be retrieved
        :param tags: tags for this download
        """
        kwargs.setdefault("url", None)
        kwargs.setdefault("tags", None)
        super(DownloadTask, self).__init__(**kwargs)


class ScrapingTask(BaseTask):
    __slots__ = []

    def __init__(self, **kwargs):
        """
        A scraping task.

        :param url: URL from which the page was retrieved
        :param tags: tags for this extraction
        :param response: HTTP response for the page
        """
        kwargs.setdefault("url", None)
        kwargs.setdefault("tags", None)
        kwargs.setdefault("response", None)
        super(ScrapingTask, self).__init__(**kwargs)


class BaseObject(dict):
    """Base for the objects retrieved by the scraper"""

    __slots__ = []  # We don't want attributes

    def __init__(self, **kwargs):
        self.update(kwargs)

    def __repr__(self):
        return "{0}({1})".format(
            '.'.join((self.__class__.__module__, self.__class__.__name__)),
            ', '.join('{0}={1!r}'.format(name, value)
                      for name, value in self.iteritems()))

    def __getstate__(self):
        """When pickling, we don't care about attributes"""
        return tuple(self.iteritems())

    def __setstate__(self, state):
        self.update(state)


class RetryTask(Exception):
    """Ask for the task to be retrieved"""
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
        self._downloaders = []
        self._scrapers = []

        ## Keep a list of already downloaded
        ## URLs to prevent infinite recursion.
        ## todo: we need a smarter way to do this..
        self._already_done = set()

    ## Register decorators
    ##------------------------------------------------------------

    def _register_decorator(self, register, func=None, **kw):
        def decorator(f):
            @wraps(f)
            def wrapper(*a, **kw):
                return f(*a, **kw)
            wrapper.__dict__.update(kw)
            register.append(wrapper)
            return wrapper
        if func is not None:
            return decorator(func)
        return decorator

    def downloader(self, func=None, **kw):
        kw.setdefault('urls', [])
        kw.setdefault('tags', [])
        return self._register_decorator(self._downloaders, func=func, **kw)

    def scraper(self, func=None, **kw):
        kw.setdefault('urls', [])
        kw.setdefault('tags', [])
        return self._register_decorator(self._scrapers, func=func, **kw)

    ## Public interface methods
    ##------------------------------------------------------------

    def run_task(self, task):
        """Run a given task"""
        if isinstance(task, DownloadTask):
            return self._run_download_task(task)
        if isinstance(task, ScrapingTask):
            return self._run_scraping_task(task)
        raise TypeError("This task doesn't look like a task..")

    def queue_task(self, task):
        """Queue a task for later execution"""
        self._task_queue.push(task)

    def pop_task(self):
        """Get next task from the queue"""
        return self._task_queue.pop()

    def run(self):
        """Start execution of the queue, until no tasks are left"""
        while True:
            task = self.pop_task()
            if task is None:
                return  # nothing left..
            self.run_task(task)

    def _runner_ok_for_task(self, runner, task):
        """Check whether the runner is ok for running a task"""

        ## If the task has a URL and the runner define
        ## filters based on URLs, make sure the url matches
        if task['url'] and runner.urls:
            if not any(re.match(u, task['url']) for u in runner.urls):
                return False

        ## If the task and the runner define tags, make sure
        ## they have at least a tag in common
        if task['tags'] and runner.tags:
            if not any(t in task['tags'] for t in runner.tags):
                return False

        ## Ok, this runner is suitable
        return True

    def _find_downloaders(self, task):
        """Yield suitable downloaders for this task"""
        ## todo: we should choose the "most matching" here..
        for downloader in self._downloaders:
            if self._runner_ok_for_task(downloader, task):
                yield downloader

    def _run_download_task(self, task):
        logger.debug("Running download task: {0!r}".format(task))

        if task in self._already_done:
            logger.info("  -> Task already processed: {0}".format(task['url']))

        for downloader in self._find_downloaders(task):
            try:
                self._wrap_task_execution(downloader, task)
            except SkipRunner:
                logger.debug("  -> Downloader asked to be skipped. "
                             "Continuing on with next one.")
                pass
            else:
                break

        self._already_done.add(task)

    def _find_scrapers(self, task):
        """
        Yield all the scrapers with matching URLs and that
        share at least one tag with the task.
        """
        for scraper in self._scrapers:
            if self._runner_ok_for_task(scraper, task):
                yield scraper

    def _run_scraping_task(self, task):
        logger.debug("Running scraping task: {0!r}".format(task))
        for scraper in self._find_scrapers(task):
            logger.debug("  -> Scraping with: {0!r}".format(scraper))
            try:
                self._wrap_task_execution(scraper, task)
            except SkipRunner:  # we can safely ignore this..
                pass

    def _wrap_task_execution(self, runner, task):
        logger.info("Starting task: {0!r} (via {1!r})".format(task, runner))
        try:
            for item in runner(task):
                if isinstance(item, BaseTask):
                    logger.debug("  -> Got new task {0!r}".format(item))
                    self.queue_task(item)
                elif isinstance(item, BaseObject):
                    logger.debug("  -> Got new object: {0!r}".format(item))
                    self._store(item)
                else:
                    logger.debug("  -> I don't know what to do with: {0!r}"
                                 "".format(item))
        except AbortTask:
            logger.info("Task {0!r} aborted".format(task))
        except Exception, e:
            if isinstance(e, SkipRunner):
                ## This should be handled by caller..
                raise
            if not isinstance(e, RetryTask):
                ## We retry failing tasks, but we notify the user, if the
                ## exception wasn't a RetryTask
                logger.warning("Task failed with unknown exception. Retrying.")
                logger.exception("")
            if task['retry'] > 0:
                logger.info("Task {0!r} to be retried "
                            "{1} more times".format(task, task['retry']))
                new_task = task.clone(retry=task['retry'] - 1)
                self.queue_task(new_task)
            else:
                logger.info("Max retries reached. Aborting task {0!r}.")

    def _store(self, obj):
        logger.debug("Storing object: {0!r}".format(obj))
        if self._storage is not None:
            self._storage.save(obj)

    @property
    def _storage(self):
        return self.conf.get('storage')

    @property
    def _task_queue(self):
        if not self.conf.get('queue'):
            self.conf['queue'] = ListQueueManager()
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

    def push(self, task):
        """Pushes a task to the queue"""
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

    def pop(self):
        try:
            return self._queue.pop(0)
        except IndexError:
            return None

    def push(self, task):
        self._queue.append(task)
