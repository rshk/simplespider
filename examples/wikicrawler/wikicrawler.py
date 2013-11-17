"""
Example crawler for Wikipedia
"""

import re
import urlparse

## pip install requests lxml
import requests
import lxml.html

from simplespider import DictStorageSpider, ScrapingTask, DownloadTask, \
    BaseObject, SkipRunner


spider = DictStorageSpider()

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
    if not re.match(wikipedia_page_re, url):
        return False
    return True


def is_special_page(url):
    """Check whether this wikipedia page is a "special" page"""
    p = urlparse.urlparse(url)
    path = p.path.split('/')
    assert len(path) >= 3
    assert path[:2] == ['', 'wiki']
    for x in ('Talk:', 'Help:', 'Category:', 'Template:', 'Wikipedia:',
              'User:', 'Portal:', 'Special:', 'File:'):
        if path[2].startswith(x):
            return True
    return False


class WikipediaPage(BaseObject):
    def __repr__(self):
        return "WikipediaPage({0!r}, {1!r})".format(self.url, self.title)


class WikipediaLink(BaseObject):
    def __repr__(self):
        return "WikipediaLink({0!r} -> {1!r})".format(
            self.url_from, self.url_to)


@spider.downloader(urls=[wikipedia_re])
def wikipedia_downloader(task):
    assert isinstance(task, DownloadTask)
    if is_wikipedia_page(task.url) and is_special_page(task.url):
        raise SkipRunner()
    yield ScrapingTask(
        url=task.url,
        response=requests.get(task.url),
        tags=['wikipedia'])


@spider.scraper(urls=[wikipedia_page_re], tags=['wikipedia'])
def wikipedia_scraper(task):
    assert isinstance(task, ScrapingTask)
    if is_special_page(task.url):
        ## We only process normal pages
        raise SkipRunner()
    tree = lxml.html.fromstring(task.response.content)
    el = tree.xpath('//h1[@id="firstHeading"]')[0]
    yield WikipediaPage(url=task.url, title=el.text_content())
    base_url = task.url
    links = set(clean_url(urlparse.urljoin(base_url, x))
                for x in tree.xpath('//a/@href'))
    for link in links:
        if is_wikipedia_page(link) and (not is_special_page(link)):
            yield WikipediaLink(url_from=base_url, url_to=link)


@spider.scraper
def simple_link_extractor(task):
    assert isinstance(task, ScrapingTask)
    tree = lxml.html.fromstring(task.response.content)
    base_url = task.url
    links = set(clean_url(urlparse.urljoin(base_url, x))
                for x in tree.xpath('//a/@href'))
    for link in links:
        yield DownloadTask(url=link)


if __name__ == '__main__':
    try:
        task = DownloadTask(url='http://en.wikipedia.org')
        spider.queue_task(task)
        spider.run()
    except KeyboardInterrupt:
        print("\n\n----\nTerminated by user.\nPrinting report\n")
        for table in spider._storage:
            print("{0}: {1} objects".format(
                table, len(spider._storage[table])))
