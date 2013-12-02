"""
Web-crawler related stuff
"""

import cgi
import logging
import re
import urlparse

import lxml
import requests
import requests.utils

from simplespider import BaseTask, BaseTaskRunner

logger = logging.getLogger(__name__)


def default_user_agent():
    return ''.join('spydi')
    return requests.utils.default_user_agent()


class HttpResponse(dict):
    def __init__(self, **kwargs):
        kwargs.setdefault('headers', {})
        kwargs.setdefault('content', '')
        kwargs.setdefault('encoding', None)
        kwargs.setdefault('ok', True)
        kwargs.setdefault('status_code', None)
        kwargs.setdefault('reason', '')
        kwargs.setdefault('url', '')
        self.update(kwargs)

    def __repr__(self):
        return "<HttpResponse {0!r} ({1!r})>".format(
            self['status_code'], self['url'])


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


class Downloader(BaseTaskRunner):
    def __init__(self, **kwargs):
        kwargs.setdefault('max_depth', 0)  # 0 means "infinite"
        kwargs.setdefault('allow_redirects', True)
        kwargs.setdefault('user_agent', True)
        super(Downloader, self).__init__(**kwargs)

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

        ## We need to serialize this!
        response_dict = HttpResponse(
            headers=dict(response.headers),
            content=response.content,
            encoding=response.encoding,
            ok=response.ok,
            status_code=response.status_code,
            reason=response.reason,
            url=response.url,  # might change
        )

        ## Keep history of the followed "trail"
        trail = task.get('trail') or []

        yield ScrapingTask(
            url=task['url'],
            trail=trail,
            response=response_dict,
            tags=['wikipedia'])


class BaseScraper(BaseTaskRunner):
    def match(self, task):
        return isinstance(task, ScrapingTask)


class LinkExtractor(BaseScraper):
    def __init__(self, **kwargs):
        kwargs.setdefault('find_urls_in_text', True)
        kwargs.setdefault('deduplicate_links', True)
        super(LinkExtractor, self).__init__(**kwargs)

        url_schemas = '|'.join(('http', 'https'))
        url_chars = 'a-zA-Z0-9' + re.escape(':/&?')
        self.re_url = re.compile(
            '((?:{schema})://[{url_chars}]+)'.format(
                schema=url_schemas, url_chars=url_chars))

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

    def _find_urls_in_text(self, text):
        for url in self.re_url.findall(text):
            url = url.rstrip('.,;:')
            yield url

    def _extract_links(self, response):
        content_type, params = cgi.parse_header(
            response['headers'].get('content-type') or 'text/html')

        if content_type == 'text/html':
            tree = lxml.html.fromstring(response['content'])
            base_url = response['url']
            links = self._prepare_urls(
                urlparse.urljoin(base_url, x)
                for x in tree.xpath('//a/@href'))
            for link in links:
                yield link

        elif content_type.startswith('text/'):
            if self.conf['find_urls_in_text']:
                for link in self._find_urls_in_text(response['content']):
                    yield link

    def __call__(self, task):
        assert self.match(task)
        response = task['response']

        ## Trail that was followed to find this link
        trail = []
        if task.get('trail'):
            trail.extend(task['trail'])
        trail.append(task['url'])
        if response['url'] != task['url']:
            trail.append(response['url'])

        ## Extract all links in this page
        links = self._extract_links(response)
        if self.conf['deduplicate_links']:
            links = set(links)

        ## todo: we could also "extract" links by going uphill
        ## along the path, remove GET arguments, etc..

        for link in links:
            yield DownloadTask(url=link, trail=trail)
