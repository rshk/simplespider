import os

import pytest

from simplespider import ListQueueManager


@pytest.fixture(params=['list', 'kombu_simple'])
def queue(request):
    if request.param == 'list':
        return ListQueueManager()

    if request.param == 'kombu_simple':
        KOMBU_URL = os.environ.get('KOMBU_URL')
        if not KOMBU_URL:
            pytest.skip()

        from simplespider.queues.kombu import KombuQueueSimple
        ## todo: retrieve connection url from environment, skip
        ## if not available..
        return KombuQueueSimple(connection=KOMBU_URL)
