"""
Tests for the task objects
"""

import sys

import pytest

from simplespider import BaseTask, DownloadTask, ScrapingTask


@pytest.fixture(params=[BaseTask, DownloadTask, ScrapingTask])
def task_class(request):
    return request.param


@pytest.fixture(params=['json', 'pickle', 'msgpack'])
def serializer_module(request):
    if request.param == 'json':
        import json
        return json
    elif request.param == 'pickle':
        # if sys.version_info >= (3,):
        #     pytest.xfail("Pickling tasks in Python 3 is known not to work")
        import pickle
        return pickle
    else:
        pytest.skip("Unsupported serializer {0}".format(request.param))
