"""
Storage task processors
"""

from collections import defaultdict
import anydbm
import json
import uuid

from simplespider import BaseTask, BaseTaskRunner


class StoreObjectTask(BaseTask):
    def __init__(self, task_id=None, **kwargs):
        if not kwargs.get('data'):
            raise TypeError("The 'data' argument is required!")
        super(StoreObjectTask, self).__init__(task_id, **kwargs)


class DictStorage(BaseTaskRunner):
    """
    Simple storage, keeping all the data in a dictionary
    in the form ``{type: [items]}``
    """

    def __init__(self, **kwargs):
        super(DictStorage, self).__init__(**kwargs)
        self._storage = defaultdict(list)

    def match(self, task):
        return isinstance(task, StoreObjectTask)

    def __call__(self, task):
        obj = task['data']
        obj_type = obj.get('_type') or type(obj).__name__
        self._storage[obj_type].append(obj)


class AnydbmStorage(BaseTaskRunner):
    """
    Storage backed by an anydbm database.
    """

    def __init__(self, **kwargs):
        if not kwargs.get('path'):
            raise TypeError("The 'path' argument is required!")
        kwargs.setdefault('synchronous', True)

        super(AnydbmStorage, self).__init__(**kwargs)

        self._storage_path = kwargs['path']
        self._storage = anydbm.open(self._storage_path, 'c')

    def match(self, task):
        return isinstance(task, StoreObjectTask)

    def __call__(self, task):
        obj = task['data']
        obj['_type'] = obj.get('_type') or type(obj).__name__
        obj['_id'] = obj.get('_id') or obj.get('id') or str(uuid.uuid4())
        storage_key = "{0}.{1}".format(obj['_type'], obj['_id'])
        self._storage[storage_key] = json.dumps(obj)
        if self.conf['synchronous']:
            try:
                self._storage.sync()
            except AttributeError:  # pragma: no cover
                pass  # On Py3k this method disappeared..
