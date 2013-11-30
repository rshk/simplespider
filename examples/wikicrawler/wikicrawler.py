"""
Example crawler for Wikipedia
"""

import logging
import re
import sys
import urlparse

## pip install requests lxml
import requests
import lxml.html

from simplespider import Spider, DictStorage, AnydbmStorage, \
    BaseObject, BaseTaskRunner
from simplespider.web import DownloadTask, DownloadTaskRunner, \
    ScrapingTask, LinkExtractionRunner

logger = logging.getLogger('simplespider.examples.wikicrawler')


wikipedia_home_re = ''.join(('^', re.escape('http://en.wikipedia.org'), '/?$'))
wikipedia_page_re = ''.join((
    '^', re.escape('http://en.wikipedia.org/wiki/'), '.*'))
wikipedia_re = '|'.join((wikipedia_home_re, wikipedia_page_re))


def clean_url(url):
    ## Strip any #fragment part
    return url.split('#', 1)[0]


def is_wikipedia_page(url):
    """
    Check whether this is page is part of wikipedia "interesting"
    pages (ie. the ones matching the above regexp)
    """
    if not re.match(wikipedia_re, url):
        return False
    return True


def is_special_page(url):
    """Check whether this wikipedia page is a "special" page"""
    p = urlparse.urlparse(url)
    path = p.path.split('/')
    if len(path) < 3:
        return False
    if path[:2] != ['', 'wiki']:
        return False
    for x in ('Talk:', 'Help:', 'Category:', 'Template:', 'Wikipedia:',
              'User:', 'Portal:', 'Special:', 'File:'):
        if path[2].startswith(x):
            return True
    return False


class WikipediaPage(BaseObject):
    pass


class WikipediaLink(BaseObject):
    pass


class WikipediaDownloader(DownloadTaskRunner):
    def match(self, task):
        if not super(WikipediaDownloader, self).match(task):
            return False
        if not is_wikipedia_page(task['url']):
            logger.debug("Not a wikipedia page: " + task['url'])
            return False
        if is_special_page(task['url']):
            logger.debug("This is a special page")
            return False
        return True


class WikipediaScraper(BaseTaskRunner):
    def match(self, task):
        if not super(WikipediaScraper, self).match(task):
            return False
        if not is_wikipedia_page(task['url']):
            return False
        if is_special_page(task['url']):
            return False
        return True

    def __call__(self, task):
        assert self.match(task)
        content_type = task['response'].headers['Content-type'].split(';')
        if content_type[0] != 'text/html':
            return  # Nothing to do here..
        tree = lxml.html.fromstring(task['response'].content)
        el = tree.xpath('//h1[@id="firstHeading"]')[0]
        yield WikipediaPage(url=task['url'], title=el.text_content())


spider = Spider()
spider.add_runners([
    WikipediaDownloader(),
    WikipediaScraper(),
    LinkExtractionRunner(),
])


if __name__ == '__main__':
    try:
        ## Prepare the storage
        if len(sys.argv) > 1:
            storage = AnydbmStorage(sys.argv[1])
        else:
            storage = DictStorage()
        spider.conf['storage'] = storage

        ## Queue the first task
        task = DownloadTask(url='http://en.wikipedia.org')
        spider.queue_task(task)

        ## Run!
        spider.run()

    except KeyboardInterrupt:
        print("\n\n----\nTerminated by the user.")
