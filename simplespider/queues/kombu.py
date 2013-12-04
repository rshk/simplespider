"""
Kombu-based tasks queue
"""

from __future__ import absolute_import

from kombu import Connection

from simplespider import BaseQueueManager, BaseTask
from simplespider.utils import lazy_property


class KombuQueueSimple(BaseQueueManager):
    """Queue based on kombu.simple queues"""

    def __init__(self, **kwargs):
        """
        :param connection:
            a kombu Connection instance
        :param queue_name:
            name to be used for the queue. (Default: 'simplespider_tasks')
        :param serializer:
            serializer to be used for tasks. (Default: 'json')
        :param compression:
            compression to be used. (Default: None)
        """
        if 'connection' not in kwargs:
            raise TypeError("The 'connection' argument is required!")
        if isinstance(kwargs['connection'], basestring):
            kwargs['connection'] = Connection(kwargs['connection'])
        kwargs.setdefault('queue_name', 'simplespider_tasks')
        kwargs.setdefault('serializer', 'json')
        kwargs.setdefault('compression', None)
        super(KombuQueueSimple, self).__init__(**kwargs)

    @property
    def connection(self):
        return self.conf['connection']

    @lazy_property
    def queue(self):
        return self.connection.SimpleQueue(self.conf['queue_name'])

    def pop(self):
        ## We need to deserialize the task..
        message = self.queue.get(block=True, timeout=1)
        raw_task = message.payload
        task = BaseTask.from_dict(raw_task)
        message.ack()
        return task.id, task

    def push(self, name, task):
        assert name == task.id
        self.queue.put(task.to_dict(),
                       serializer=self.conf['serializer'],
                       compression=self.conf['compression'])

    def __len__(self):
        return len(self.queue)
