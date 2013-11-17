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
from functools import wraps
import logging
import re
import sys


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter(
    "%(levelname)s %(filename)s:%(lineno)d %(funcName)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class BaseTask(object):
    def __init__(self, **kwargs):
        ## to get started quickly..
        self.__dict__.update(kwargs)


class DownloadTask(BaseTask):
    def __init__(self, url, **kw):
        super(DownloadTask, self).__init__(**kw)
        self.url = url

    def __repr__(self):
        return "DownloadTask(url={0!r})".format(self.url)


class ScrapingTask(BaseTask):
    def __init__(self, url, response, tags=None, **kw):
        super(ScrapingTask, self).__init__(**kw)
        self.url = url
        self.response = response
        self.tags = tags or []

    def __repr__(self):
        return "DownloadTask(url={0!r}, tags={1!r})".format(
            self.url, self.tags)


class BaseObject(object):
    """Base for the objects retrieved by the scraper"""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Spider(object):
    def __init__(self):
        self._downloaders = []
        self._scrapers = []
        ## Keep a list of already downloaded
        ## URLs to prevent infinite recursion.
        ## todo: we need a smarter way to do this..
        self._already_done = set()
        self._tasks_queue = []
        self._storage = defaultdict(list)

    def downloader(self, func=None, **kw):
        def decorator(f):
            @wraps(f)
            def wrapper(*a, **kw):
                return f(*a, **kw)
            wrapper.__dict__.update(kw)
            self._downloaders.append(wrapper)
            return wrapper
        if func is not None:
            return decorator(func)
        return decorator

    def scraper(self, func=None, **kw):
        def decorator(f):
            @wraps(f)
            def wrapper(*a, **kw):
                return f(*a, **kw)
            wrapper.__dict__.update(kw)
            self._scrapers.append(wrapper)
            return wrapper
        if func is not None:
            return decorator(func)
        return decorator

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

    def _find_downloader(self, url):
        ## The first downloader matching is used
        ## todo: we should choose the "most matching" here..
        for downloader in self._downloaders:
            for d_url in downloader.urls:
                if re.match(d_url, url):
                    return downloader

    def _run_download_task(self, task):
        logger.debug("Running download task: {0!r}".format(task))
        if task.url in self._already_done:
            logger.info("  -> URL already processed: {0}".format(task.url))
        downloader = self._find_downloader(task.url)
        if downloader is None:  # Nothing to do..
            logger.info("  -> No downloader found. Skipping.")
            return
        self._wrap_task_execution(downloader, task)
        self._already_done.add(task.url)

    def _find_scrapers(self, task):
        # Find all scrapers with matching URLs and that
        # share at least one tag with the task.
        for scraper in self._scrapers:
            if any(re.match(u, task.url) for u in scraper.urls):
                if any(t in task.tags for t in scraper.tags):
                    yield scraper

    def _run_scraping_task(self, task):
        logger.debug("Running scraping task: {0!r}".format(task))
        for scraper in self._find_scrapers(task):
            logger.debug("  -> Scraping with: {0!r}".format(scraper))
            self._wrap_task_execution(scraper, task)

    def _wrap_task_execution(self, runner, task):
        logger.debug("Starting task: {0!r} (via {1!r})".format(task, runner))
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

    def _store(self, obj):
        logger.debug("Storing object: {0!r}".format(obj))
        self._storage[type(obj).__name__].append(obj)
