"""
Tests for the task objects
"""

import pytest

from simplespider import BaseTask, DownloadTask, ScrapingTask


@pytest.fixture(params=[BaseTask, DownloadTask, ScrapingTask])
def task_class(request):
    return request.param


def test_base_task_functionality(task_class):
    task = task_class(hello='world', foo='bar')
    assert len(task) >= 2  # We have some default attributes too..
    assert 'hello' in task
    assert 'foo' in task
    assert task['hello'] == 'world'
    assert task['foo'] == 'bar'

    iterated = list(iter(task))
    assert iterated == list(task.iterkeys())
    assert 'hello' in iterated
    assert 'foo' in iterated

    keys = task.keys()
    assert isinstance(keys, list)
    assert 'hello' in keys
    assert 'foo' in keys

    iterated = list(task.iteritems())
    assert ('hello', 'world') in iterated
    assert ('foo', 'bar') in iterated

    items = task.items()
    assert isinstance(items, list)
    assert ('hello', 'world') in items
    assert ('foo', 'bar') in items


#@pytest.mark.parametrize('task_class', [BaseTask, DownloadTask, ScrapingTask])
def test_tasks_are_immutable(task_class):
    """
    Make sure tasks are immutable. They can only be changed
    using the .clone(**kwargs) method (creating a new task)
    """
    task = task_class(url="http://www.example.com", retry=2)
    assert task['url'] == 'http://www.example.com'
    assert task['retry'] == 2

    with pytest.raises(TypeError):
        task['url'] = 'http://www.example.org'

    with pytest.raises(TypeError):
        del task['url']

    # Lists are not hashable
    with pytest.raises(TypeError):
        task_class(spam=['a', 'b', 'c'])

    # But tuples are
    task = task_class(spam=('a', 'b', 'c'))
    assert task['spam'] == ('a', 'b', 'c')

    # Check hasattr()
    task = task_class(spam='Hello, spam!')
    assert 'spam' in task
    assert 'eggs' not in task

    # Non-existent attributes raise AttributeError
    with pytest.raises(KeyError):
        task_class(hello='world')['foo']

    # But setting still raises TypeError
    with pytest.raises(TypeError):
        task = task_class(hello='world')
        del task['foo']

    # This is the right way to "mutate" 'em..
    assert task == task.clone()
    clone = task.clone(url='http://www.example.org')
    assert task != clone
    assert clone['url'] == 'http://www.example.org'
    assert clone['retry'] == 2

    # Attribute setting is not allowed, since we are
    # using __slots__
    task = task_class(hello='Hello, world')
    with pytest.raises(AttributeError):
        task.myattr = 'foo'


def test_task_comparison(task_class):
    """
    Make sure tasks with identical attributes compare as equal
    """

    task = task_class(url='http://www.example.com', retry=2)
    task1 = task_class(url='http://www.example.com', retry=2)
    task2 = task_class(url='http://www.example.com', retry=1)

    assert task == task1
    assert task != task2
    assert task1 != task2

    assert len(set((task, task1, task2))) == 2

    task_clone = task.clone()
    task_clone_r1 = task.clone(retry=1)
    assert task == task_clone
    assert task2 == task_clone_r1

    ## Make sure this set gets built correctly
    assert len(set((task, task1, task_clone, task_clone_r1))) == 2

    ## Test comparison with different class, same arguments
    for task_class_2 in (BaseTask, DownloadTask, ScrapingTask):
        if type(task_class) != task_class_2:
            assert task_class(url='http://www.example.com') != \
                task_class_2(url='http://www.example.com')


def test_multi_task_set():
    assert len(set((
        BaseTask(url='http://www.example.com'),
        BaseTask(url='http://www.example.com'),
    ))) == 1
    assert len(set((
        BaseTask(url='http://www.example.com'),
        DownloadTask(url='http://www.example.com'),
        ScrapingTask(url='http://www.example.com'),
        BaseTask(url='http://www.example.com'),
    ))) == 3
    assert len(set((
        BaseTask(url='http://www.example.com'),
        BaseTask(url='http://www.example.com', foo='bar'),
        DownloadTask(url='http://www.example.com'),
        ScrapingTask(url='http://www.example.com', foo='bar'),
        BaseTask(url='http://www.example.com'),
        ScrapingTask(url='http://www.example.com', foo='bar'),
    ))) == 4


def test_mutable_hack(task_class):
    """
    This is a known quirk: we can still mutate the task,
    by setting its _BaseTask__attributes attribute.
    This is bad, but not easy to prevent..
    """

    task = task_class(foo='bar')

    with pytest.raises(TypeError):
        task['foo'] = 'baz'

    ## Let's hack this..
    task._BaseTask__attributes = frozenset((('foo', 'baz'), ('spam', 'eggs')))
    assert task['foo'] == 'baz'
    assert task['spam'] == 'eggs'

    ## Even worse, we can do this:
    task._BaseTask__attributes = 'F*ck you!'
    with pytest.raises(ValueError):
        assert task['foo'] == 'baz'  # :(


def test_task_serialization(task_class):
    ## We can pickle tasks
    import pickle

    task = task_class(url='http://www.example.com', retry=2)
    pickled = pickle.dumps(task)
    task1 = pickle.loads(pickled)
    assert task == task1
    assert task1['url'] == 'http://www.example.com'
    assert task1['retry'] == 2
