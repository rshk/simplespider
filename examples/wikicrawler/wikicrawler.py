"""
Example crawler for Wikipedia
"""

import re
import urlparse

## pip install requests lxml
import requests
import lxml.html

from simplespider import Spider, ScrapingTask, DownloadTask, BaseObject


spider = Spider()

wikipedia_home_re = ''.join(('^', re.escape('http://en.wikipedia.org'), '/?$'))
wikipedia_page_re = ''.join((
    '^', re.escape('http://en.wikipedia.org/wiki/'), '.*'))
wikipedia_re = '|'.join((wikipedia_home_re, wikipedia_page_re))


def clean_url(url):
    ## Strip any #fragment part
    return url.split('#', 1)[0]


def is_wikipedia_page(url):
    if not re.match(wikipedia_page_re, url):
        return False
    p = urlparse.urlparse(url)
    path = p.path.split('/')
    if len(path) < 3:  # ['', 'wiki', 'Title']
        return False
    for x in ('Talk:', 'Help:', 'Category:', 'Template:', 'Wikipedia:',
              'User:', 'Portal:', 'Special:'):
        if path[2].startswith(x):
            return False
    return True


class WikipediaPage(BaseObject):
    def __repr__(self):
        return "WikipediaPage({0!r}, {1!r})".format(self.url, self.title)


class WikipediaLink(BaseObject):
    def __repr__(self):
        return "WikipediaLink({0!r} -> {1!r})".format(
            self.url_from, self.url_to)


@spider.downloader(
    urls=[wikipedia_re])
def wikipedia_downloader(task):
    assert isinstance(task, DownloadTask)
    yield ScrapingTask(
        url=task.url,
        response=requests.get(task.url),
        tags=['wikipedia'])


@spider.scraper(
    urls=[wikipedia_re],
    tags=['wikipedia'])
def wikipedia_scraper(task):
    assert isinstance(task, ScrapingTask)
    tree = lxml.html.fromstring(task.response.content)
    el = tree.xpath('//h1[@id="firstHeading"]')[0]
    yield WikipediaPage(url=task.url, title=el.text_content())
    base_url = task.url
    links = set(clean_url(urlparse.urljoin(base_url, x))
                for x in tree.xpath('//a/@href'))
    for link in links:
        if is_wikipedia_page(link):
            yield WikipediaLink(url_from=base_url, url_to=link)
        yield DownloadTask(link, tags=['wikipedia'])


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
