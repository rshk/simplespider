# Simple Spider

A library to build simple yet powerful spiders, in Python.

[![Build Status](https://travis-ci.org/rshk/simplespider.png?branch=master)](https://travis-ci.org/rshk/simplespider)
[![Coverage Status](https://coveralls.io/repos/rshk/simplespider/badge.png)](https://coveralls.io/r/rshk/simplespider)
[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/rshk/simplespider/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

-----

**Warning!**
This thing is still experimental and the API might change in the future.
Please do not use for anything "serious" yet. Any feedback / use case /
contribution is highly appreciated.

-----

**Note:** still looking for a better name :)

-----

## Principles

The project goal is to build a library that offers a lot of functionality
required to build a proper, distributed, web crawler, without getting too much
in the way of the user.

The core is very minimal (<400 SLOC at time of writing), but defines a "common
ground" on which to plug functionality from other sub-modules, or third party.

Differently from other web crawling libraries, it doesn't assume anything about
what information you're trying to gather, the workflow you want to follow
or the technologies involved.

Although in many cases you'll want to get some http pages, extract links from html
and follow those links recursively, you might want to use your libraries of choice
to do so, or you might be looking for error pages (thus not to be considered just
"errors" to be retried), or you might want to use a protocol that's not http, ...

You should be allowed to do all that, without having to re-write a lot of common
functionality.


## Core functionality

The core simply keeps a register of "task processors" and a queue of "tasks".

During processing, it will loop something like this:

1. Pop a task from the queue
2. Loop over processors, calling their ``match()`` method to see if they
   accept the job
   1. For each processor accepting the job, iterate over ``processor(task)``
   2. Yielded tasks are put in the queue
   3. Yielded objects are stored in a database
   4. Exceptions are handled in different ways:
      - ``AbortTask`` indicates that execution should stop here, no more
	    processors will be used
	  - ``SkipRunner`` this runner required to be skipped; jump to the
	    next one
	  - ``RetryTask`` something went wrong, but the task should be retried later
	  - other exceptions will trigger a retry too (along with a warning message)
3. Executions continue looping from ``1``, until queue is empty.


## Internal API

``Tasks`` are simply wrapper around a dictionary. They have an ``id``, used for
de-duplication in queues, a ``type`` (that is the runner ``module.Class``)
and some variable attributes (kwargs to constructor).

``Objects`` are just dicts, with a custom sub-type, mostly used to guarantee
consitency and do type checking.

``TaskRunners`` provide a very simple interface:

* ``match(task)``

  Returns ``True`` if the task should be run using this runner,
  ``False`` otherwise.

* ``__call__(task)``

  Run to trigger task execution with this runner.

  This is a generator, yielding objects and/or tasks.

  Also, there are some exceptions that can be raised to control the
  execution flow (eg. skip/abort/retry the running task).

Last but not least, the ``Spider`` class provides the following methods:

* ``add_runners(runners)`` to register a list of runners

* ``queue_task(task)`` to add a task to the queue

* ``run()`` to start queue execution


## Example: extracting relations from wikipedia

The full code for this example is available in ``examples/wikicrawler/wikicrawler.py``.

To run the example, you need to ``pip install requests lxml``.

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
