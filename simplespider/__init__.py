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
import copy
from functools import wraps
import logging
import re
import sys


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


class SimpleObject(object):
    """
    Base class for objects that allow simple comparation,
    mainly in order to be put in a set.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return "{0}({1})".format(
            '.'.join((self.__class__.__module__, self.__class__.__name__)),
            ', '.join('{0}={1!r}'.format(name, value)
                      for name, value in self.__dict__.iteritems()))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__

    def clone(self, **kwargs):
        new_kwargs = copy.deepcopy(self.__dict__)
        new_kwargs.update(kwargs)
        return self.__class__(**new_kwargs)


class BaseTask(SimpleObject):
    def __init__(self, **kwargs):
        """
        Base for the spider tasks.

        :param retry:
            Number of times this task should be retried upon failure.
            Defaults to 2 (for a total of 3 exectutions).
        """
        kwargs.setdefault("retry", 2)
        self.__dict__.update(kwargs)


class DownloadTask(BaseTask):
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


class BaseObject(SimpleObject):
    """Base for the objects retrieved by the scraper"""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


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
    def __init__(self):
        self._downloaders = []
        self._scrapers = []
        ## Keep a list of already downloaded
        ## URLs to prevent infinite recursion.
        ## todo: we need a smarter way to do this..
        self._already_done = set()
        self._tasks_queue = []

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

    def run_task(self, task):
        if isinstance(task, DownloadTask):
            return self._run_download_task(task)
        if isinstance(task, ScrapingTask):
            return self._run_scraping_task(task)
        raise TypeError("This task doesn't look like a task..")

    def queue_task(self, task):
        self._tasks_queue.append(task)

    def run(self):
        while True:
            try:
                task = self._tasks_queue.pop(0)
            except IndexError:
                return  # We're done..
            self.run_task(task)

    def _runner_ok_for_task(self, runner, task):
        """Check whether the runner is ok for running a task"""

        ## If the task has a URL and the runner define
        ## filters based on URLs, make sure the url matches
        if task.url and runner.urls:
            if not any(re.match(u, task.url) for u in runner.urls):
                return False

        ## If the task and the runner define tags, make sure
        ## they have at least a tag in common
        if task.tags and runner.tags:
            if not any(t in task.tags for t in runner.tags):
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
            logger.info("  -> Task already processed: {0}".format(task.url))

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
            if task.retry > 0:
                logger.info("Task {0!r} to be retried "
                            "{1} more times".format(task, task.retry))
                new_task = task.clone(retry=task.retry - 1)
                self.queue_task(new_task)
            else:
                logger.info("Max retries reached. Aborting task {0!r}.")

    def _store(self, obj):
        logger.debug("Storing object: {0!r}".format(obj))


class DictStorageSpider(Spider):
    def __init__(self):
        super(DictStorageSpider, self).__init__()
        self._storage = defaultdict(list)

    def _store(self, obj):
        super(DictStorageSpider, self)._store(obj)
        self._storage[type(obj).__name__].append(obj)
