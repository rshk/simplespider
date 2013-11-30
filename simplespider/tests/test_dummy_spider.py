"""
Test with dummy spider operations
"""

import json
import re
import urlparse

from simplespider import Spider, DictStorage, BaseTaskRunner
from simplespider.web import DownloadTask, ScrapingTask, DownloadTaskRunner


def _json_page(content):
    return {
        'status': 200,
        'headers': {'Content-type': 'application/json'},
        'content': json.dumps(content),
    }


def _html_page(content):
    return {
        'status': 200,
        'headers': {'Content-type': 'text/html'},
        'content': content,
    }


class FakeWebsite(object):
    pages = {
        'http://www.example.com': _json_page({
            'links': [
                '/page1.json',
                '/page2.json#fragment',
                '//www.example.com/otherpage.html',
                '//othersite.com/otherpage.html',
                '//othersite.com/page1.json',
                'http://othersite.example.com/foo/bar.html',
                'http://dummy.example.com/index.html',
                '/broken-link.html',
            ],
        }),
        'http://www.example.com/page1.json': _json_page({
            'links': [
                '/page1.json',
                '/page2.json',
                '/page2.json#fragment-1',
                '/page2.json#fragment-2',
                'http://do-not-follow.com/page1.html',
                'https://do-not-follow.com/page2.html',
                '//do-not-follow.com/page3.html',
            ],
        }),
        'http://www.example.com/page1.json?lang=it': _json_page({
            'links': [
                '/it/page1.json',
                '/it/page2.json',
                '/it/page2.json#fragment-1',
                '/it/page2.json#fragment-2',
                'http://do-not-follow.com/it/page1.html',
                'https://do-not-follow.com/it/page2.html',
                '//do-not-follow.com/it/page3.html',
            ],
        }),
        'http://www.example.com/page2.json': _json_page({
            'links': [
                '#fragment-1',
                '#fragment-2',
                '/page2.json#fragment-1',
                '/page2.json#fragment-3',
                '/page3.json',
                '/broken-link.html',
                '/broken-link.html#frag',
            ]
        }),
        'http://www.example.com/page3.json': _json_page({
            'links': [
                '/page1.json',
                '/page1.json?lang=it',
                '/page2.json',
                '/broken-link.html',
            ]
        }),
        'http://www.example.com/otherpage.html': _html_page(
            "<html><head></head><body>Hello world</body></html>"),
    }

    def __init__(self):
        self._requests = []

    def get(self, url):
        self._requests.append(url)
        if url in self.pages:
            return self.pages[url]
        return {
            'status': 404,
            'headers': {'Content-type': 'text/plain'},
            'content': 'Page not found: {0}'.format(url),
        }

    def get_history(self):
        return self._requests


example_com_re = re.compile(
    ''.join(('^', re.escape('http://www.example.com'), '(/?.*)$')))


class ExampleComDownloader(DownloadTaskRunner):
    def match(self, task):
        if not super(ExampleComDownloader, self).match(task):
            return False
        if not example_com_re.match(task['url']):
            return False
        return True

    def __call__(self, task):
        response = self.conf['site'].get(task['url'])
        yield ScrapingTask(
            url=task['url'],
            response=response)


class ExampleComScraper(BaseTaskRunner):
    def match(self, task):
        if not super(ExampleComScraper, self).match(task):
            return False
        if not example_com_re.match(task['url']):
            return False
        return True

    def __call__(self, task):
        content_type = task.response['headers']['Content-type'].split(';')
        if content_type[0] == 'application/json':
            data = json.loads(task.response['content'])
            for link in data['links']:
                yield DownloadTask(
                    url=urlparse.urljoin(task['url'], link))


def test_dummy_spider():
    site = FakeWebsite()
    spider = Spider()

    spider.add_runners([
        ExampleComDownloader(site=site),
        ExampleComScraper(),
    ])
    storage = DictStorage()
    spider.conf['storage'] = storage
    task = DownloadTask(url='http://www.example.com')
    spider.queue_task(task)
    spider.run()

    ## Ok, now we should see what we got..
    history = site.get_history()
    assert history[0] == 'http://www.example.com'
    assert history[1] == 'http://www.example.com/page1.json'
    assert history[2] == 'http://www.example.com/page2.json'
    assert history[3] == 'http://www.example.com/otherpage.html'
