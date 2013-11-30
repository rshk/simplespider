"""
Web-crawler related stuff
"""

import urlparse
import logging

import lxml
import requests

from simplespider import BaseTask, BaseTaskRunner

logger = logging.getLogger(__name__)


class DownloadTask(BaseTask):
    __slots__ = []

    def __init__(self, **kwargs):
        """
        A downloading task.

        :param url: the URL to be retrieved
        :param retry: how many times this task should be retried.
            Defaults to 2.
        """
        kwargs.setdefault("url", None)
        kwargs.setdefault("retry", 2)
        task_id = ':'.join((self.type, kwargs['url']))
        super(DownloadTask, self).__init__(task_id, **kwargs)


class ScrapingTask(BaseTask):
    __slots__ = []

    def __init__(self, **kwargs):
        """
        A scraping task.

        :param url: URL from which the page was retrieved
        :param response: HTTP response for the page
        """
        kwargs.setdefault("url", None)
        kwargs.setdefault("response", None)
        task_id = ':'.join((self.type, kwargs['url']))
        super(ScrapingTask, self).__init__(task_id, **kwargs)


class DownloadTaskRunner(BaseTaskRunner):
    def __init__(self, **kwargs):
        kwargs.setdefault('max_depth', 3)
        super(DownloadTaskRunner, self).__init__(**kwargs)

    def match(self, task):
        if not isinstance(task, DownloadTask):
            logger.debug("Type mismatch")
            return False
        if (self.conf['max_depth'] > 0) and \
                len(task.get('trail') or []) > self.conf['max_depth']:
            logger.debug("Trail length exceeded")
            return False
        return True

    def __call__(self, task):
        assert self.match(task)
        response = requests.get(task['url'])

        ## Keep history of the followed "trail"
        trail = task.get('trail') or []

        yield ScrapingTask(
            url=task['url'],
            trail=trail,
            response=response,
            tags=['wikipedia'])


class LinkExtractionRunner(BaseTaskRunner):
    def match(self, task):
        return isinstance(task, ScrapingTask)

    def _prepare_urls(self, raw_urls):
        return self._filter_urls(self._clean_url(x) for x in raw_urls)

    def _clean_url(self, url):
        # todo: reorder query arguments too, to make unique IDs
        return url.split('#', 1)[0]

    def _filter_urls(self, urls):
        for url in urls:
            scheme = url.split(':')[0]
            if scheme not in ('http', 'https'):
                continue
            yield url

    def __call__(self, task):
        assert self.match(task)
        content_type = task['response'].headers.get(
            'Content-type', '').split(';')

        if content_type[0] == 'text/html':
            tree = lxml.html.fromstring(task['response'].content)
            base_url = task['url']

            trail = []
            if task.get('trail'):
                trail.update(task['trail'])
            trail.append(task['url'])

            links = set(self._prepare_urls(
                urlparse.urljoin(base_url, x)
                for x in tree.xpath('//a/@href')
            ))
            for link in links:
                yield DownloadTask(url=link, trail=trail)

        ## todo: for text/plain, we could extract URL look-alikes
        ## todo: we could also yield links going "uphill" along the path
