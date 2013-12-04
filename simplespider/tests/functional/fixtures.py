import os

import pytest
import six

from simplespider import ListQueueManager


@pytest.fixture(params=['list', 'kombu_simple'])
def queue(request):
    if request.param == 'list':
        return ListQueueManager()

    if request.param == 'kombu_simple':
        KOMBU_URL = os.environ.get('KOMBU_URL')

        if not KOMBU_URL:
            pytest.skip("KOMBU_URL not configured -- skipping test")

        if six.PY3:
            pytest.xfail("We have some problem with serializing tasks "
                         "on Python 3")

        from simplespider.queues.kombu import KombuQueueSimple
        ## todo: retrieve connection url from environment, skip
        ## if not available..
        return KombuQueueSimple(connection=KOMBU_URL)
