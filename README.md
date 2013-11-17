# Simple Spider

A library to build simple yet powerful spiders, in Python.

## Principles

The project goal is trying to build something that tries not to get
in the way of the user, while providing all the desired functionality.

The core is really simple, and acts as a register for "downloading"
and "crawling" tasks.

Downloaders and crawlers are just decorated functions, defined elsewhere.

Tasks are generators that can yield either objects or other tasks.

Task dispatching is done this way:

- For download tasks, a matching downloader (matching the url) is searched.
  The first one is used.

- For scraping, all the matching scrapers (on url and tags).


## Example: extracting relations from wikipedia

To run the example, you need to ``pip install requests lxml``.

(The full code is available in ``examples/wikicrawler/wikicrawler.py``).

First, we need a bunch of stuff:
```python
import re
import urlparse
import requests
import lxml.html
from simplespider import Spider, ScrapingTask, DownloadTask, BaseObject
```

Let's instantiate the "container" class:
```python
spider = Spider()
```

Let's define a regular expression to match wikipedia URLs:
```python
wikipedia_re = ''.join(('^', re.escape('http://en.wikipedia.org'), '.*'))
```

Ok, now we define a couple objects representing the objects
we want to retrieve:
```python
class WikipediaPage(BaseObject):
    def __repr__(self):
        return "WikipediaPage({0!r}, {1!r})".format(self.url, self.title)


class WikipediaLink(BaseObject):
    def __repr__(self):
        return "WikipediaLink({0!r} -> {1!r})".format(
            self.url_from, self.url_to)
```

Let's define our first downloader. It simply downloads the requested
URL and yields a scraping task:
```python
@spider.downloader(
    urls=[wikipedia_re])
def wikipedia_downloader(task):
    assert isinstance(task, DownloadTask)
    yield ScrapingTask(
        url=task.url,
        response=requests.get(task.url),
        tags=['wikipedia'])
```

And this is our first scraper: it extracts all links from the page
and yields another ``DownloadTask`` along with, if the link is
internal to wikipedia, a ``WikipediaLink`` object:
```python
@spider.scraper(
    urls=[wikipedia_re],
    tags=['wikipedia'])
def wikipedia_scraper(task):
    assert isinstance(task, ScrapingTask)
    tree = lxml.html.fromstring(task.response.content)
    el = tree.xpath('//h1[@id="firstHeading"]')[0]
    yield WikipediaPage(url=task.url, title=el.text_content())
    base_url = task.url
    links = set(urlparse.urljoin(base_url, x)
                for x in tree.xpath('//a/@href'))
    for link in links:
        if re.match(wikipedia_re, link):
            yield WikipediaLink(url_from=base_url, url_to=link)
        yield DownloadTask(link, tags=['wikipedia'])
```

Finally, some boilerplate to run the spider:
```python
if __name__ == '__main__':
    try:
        task = DownloadTask(url='http://en.wikipedia.org')
        spider.queue_task(task)
        spider.run()
    except KeyboardInterrupt:
        print("\n\n----\nTerminated by user.\nPrinting report\n\n")
        for table in spider._storage:
            print("{0}: {1} objects".format(
                table, len(spider._storage[table])))
```

The recommended way of executing this is:

```
% ipython -i wikicrawler.py
```

so that, when you Ctrl-C that, you'll drop in the interactive interpreter,
from which you can play with the ``spider`` object..
