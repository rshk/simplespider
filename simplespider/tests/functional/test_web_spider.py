"""
Tests for the "web" module
"""

import multiprocessing
import time

import flask
import pytest
import requests

from simplespider import Spider
from simplespider.web import DownloadTask, Downloader, LinkExtractor, \
    ScrapingTask
from simplespider.tests.functional.fixtures import queue  # noqa


@pytest.fixture(scope='function')
def simple_website(request):
    class MyWebsite(multiprocessing.Process):
        def __init__(self, app):
            super(MyWebsite, self).__init__()
            self._app = app

        def run(self):
            self._app.run(port=5001)

    app = flask.Flask('simple_website')

    @app.route('/')
    def homepage():
        return """
        <!DOCTYPE html>
        <html>
            <head><title>Homepage</title></head>
            <body>
                <a href="/hello">Hello, Link!</a>
                <!-- <a href="/">Homepage</a>
                <a href="/hello">Hello, Link!</a>
                <a href="/broken">This is broken</a>
                <a href="/hello">Duplicate link</a>
                <a href="/spam">Spam Page!</a> -->
            </body>
        </html>
        """

    @app.route('/hello')
    def hello():
        # name = flask.request.args.get('name', 'world')
        name = 'world'
        return """
        <!DOCTYPE html>
        <html>
            <head><title>Homepage</title></head>
            <body>
                <h1>Hello, {0}!</h1>
                <a href="/">Homepage</a>
            </body>
        </html>
        """.format(name)

    @app.route('/spam')
    def spam():
        return """
        <!DOCTYPE html>
        <html>
            <head><title>Homepage</title></head>
            <body>
                <h1>Welcome to the spam page!</h1>
                <a href="/">Homepage</a>
                <a href="/spam?hello=1">Spam1</a>
                <a href="/spam?hello=2">Spam2!</a>
                <a href="/spam?hello=3">Spam3!</a>
                <a href="/spam?hello=4">Spam4!</a>
                <a href="/hello">Hello, again!</a>
                <a href="/spam">Recursive Spam Page!</a>
            </body>
        </html>
        """

    proc = MyWebsite(app)
    proc.daemon = True

    def cleanup():
        proc.terminate()

    request.addfinalizer(cleanup)

    proc.start()
    time.sleep(.1)  # give it some time to come up..
    return proc


@pytest.fixture
def web_spider(queue):
    class LoggingSpider(Spider):
        def __init__(self, **kw):
            self._log = []
            super(LoggingSpider, self).__init__(**kw)

        def _wrap_task_execution(self, runner, task):
            self._log.append((runner, task))
            super(LoggingSpider, self)._wrap_task_execution(runner, task)

    spider = LoggingSpider(queue=queue)
    spider._testing = {
        'runners': [Downloader(), LinkExtractor()]
    }
    spider.add_runners(spider._testing['runners'])
    return spider


def test_simple_website(simple_website):
    """
    Make sure the simple website is behaving..
    """

    response = requests.get('http://127.0.0.1:5001/hello')
    assert response.ok
    assert response.status_code == 200

    response = requests.get('http://127.0.0.1:5001/does-not-exist')
    assert not response.ok
    assert response.status_code == 404


def test_simple_spider_run(simple_website, web_spider):
    web_spider.queue_task(DownloadTask(url='http://127.0.0.1:5001/'))
    #web_spider.run()

    tasks = web_spider.yield_tasks()

    downloader, scraper = web_spider._testing['runners']

    name, task = tasks.next()
    assert name == 'simplespider.web:DownloadTask:http://127.0.0.1:5001/'
    assert isinstance(task, DownloadTask)
    assert task['url'] == 'http://127.0.0.1:5001/'
    web_spider.run_task(task)
    assert web_spider._log.pop(0) == (downloader, task)

    name, task = tasks.next()
    assert name == 'simplespider.web:ScrapingTask:http://127.0.0.1:5001/'
    assert isinstance(task, ScrapingTask)
    assert task['url'] == 'http://127.0.0.1:5001/'
    web_spider.run_task(task)
    assert web_spider._log.pop(0) == (scraper, task)

    name, task = tasks.next()
    assert name == 'simplespider.web:DownloadTask:http://127.0.0.1:5001/hello'
    assert isinstance(task, DownloadTask)
    assert task['url'] == 'http://127.0.0.1:5001/hello'
    web_spider.run_task(task)
    assert web_spider._log.pop(0) == (downloader, task)

    name, task = tasks.next()
    assert name == 'simplespider.web:ScrapingTask:http://127.0.0.1:5001/hello'
    assert isinstance(task, ScrapingTask)
    assert task['url'] == 'http://127.0.0.1:5001/hello'
    web_spider.run_task(task)
    assert web_spider._log.pop(0) == (scraper, task)

    # assert len(web_spider._task_queue) == 0
    # assert len(web_spider._log) == 0
