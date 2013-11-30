# Simple Spider

A library to build simple yet powerful spiders, in Python.

[![Build Status](https://travis-ci.org/rshk/simplespider.png?branch=master)](https://travis-ci.org/rshk/simplespider)

-----

**Warning!**
Big refactoring in progress, tests are still failing.
Do not trust this thing yet!

-----

## Principles

The project goal is trying to build something that tries not to get
in the way of the user, while providing all the desired functionality.

The core is really simple, and acts as a register and dispatcher for tasks.

Tasks inherit from the ``BaseTask`` class and have a very simple interface:

* ``match(task)``

  Returns ``True`` if the task should be run using this runner,
  ``False`` otherwise.

* ``__call__(task)``

  Run to trigger task execution with this runner.

  This is a generator, yielding objects and/or tasks.

  Also, there are some exceptions that can be raised to control the
  execution flow (eg. skip/abort/retry the running task).


## Example: extracting relations from wikipedia

To run the example, you need to ``pip install requests lxml``.

(The full code is available in ``examples/wikicrawler/wikicrawler.py``).

First, we creted our custom task runners:

```python
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
```

This is just a standard downloader, what we added is the filtering
to make sure we only download pages from Wikipedia.

```python
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
```

This is our scraper, returning ``WikipediaPage`` objects (only for wikipedia
pages!)


Then, let's create the spider:

```python
spider = Spider()
spider.add_runners([
    WikipediaDownloader(),
    WikipediaScraper(),
    LinkExtractionRunner(),
])
```

And run!

```python
task = DownloadTask(url='http://en.wikipedia.org')
spider.queue_task(task)
spider.run()
```

Side note: The recommended way of running the ``wikicrawler.py`` script is:

```
% ipython -i wikicrawler.py
```

so that, when execution terminates (or you hit ``Ctrl-C``), you'll drop in the
interactive interpreter, from which you can play with the ``spider`` object
(and even re-start execution, by calling ``spider.run()``).
