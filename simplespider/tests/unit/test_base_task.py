import pytest
import six

from simplespider import BaseTask


@pytest.fixture
def task():
    return BaseTask('task-001', url='http://example.com', foo='bar')


def test_basetask_init(task):
    assert task.id == 'task-001'
    assert task['url'] == 'http://example.com'
    assert task['foo'] == 'bar'

    assert task.to_dict() == {
        '_id': 'task-001',
        '_type': 'simplespider.BaseTask',
        'retry': 2,
        'url': 'http://example.com',
        'foo': 'bar',
    }

    assert task.get('foo') == 'bar'
    assert task.get('spam') is None
    assert task.get('spam', 'eggs') == 'eggs'

    with pytest.raises(KeyError):
        task['invalid']


def test_basetask_update(task):
    task['foo'] = 'baz'
    assert task['foo'] == 'baz'

    task.update({'foo': 'new_foo', 'spam': 'eggs'})
    assert task['foo'] == 'new_foo'
    assert task['spam'] == 'eggs'

    assert task.to_dict() == {
        '_id': 'task-001',
        '_type': 'simplespider.BaseTask',
        'retry': 2,
        'url': 'http://example.com',
        'foo': 'new_foo',
        'spam': 'eggs',
    }


def test_basetask_delete(task):
    assert task['foo'] == 'bar'
    del task['foo']
    with pytest.raises(KeyError):
        task['foo']


def test_basetask_new_key(task):
    ## Try adding a key
    task['new_key'] = 'myvalue'
    assert task['new_key'] == 'myvalue'
    del task['new_key']
    with pytest.raises(KeyError):
        task['new_key']


def test_basetask_validation():
    with pytest.raises(TypeError):
        BaseTask(123)
    with pytest.raises(TypeError):
        BaseTask(['a', 'b', 'c'])
    # my_task = BaseTask(u'unicode_name')
    # assert my_task.id == b'unicode_name'
    # assert isinstance(my_task.id, six.binary_type)


def test_basetask_repr(task):
    assert repr(task) == "simplespider.BaseTask('task-001', "\
        "foo='bar', retry=2, url='http://example.com')"


def test_task_pickle_serializable(task):
    import pickle
    serialized = pickle.dumps(task)
    new_task = pickle.loads(serialized)
    assert task == new_task


# def test_task_json_serializable(task):
#     import json
#     serialized = json.dumps(task.to_dict())
#     # de-serialization is trickier, as we need to find the
#     # object specified in _type and instantiate it with
#     # new keys.
#     pass
