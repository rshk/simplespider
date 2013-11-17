"""
Tests for the objects retrieved by the spider
"""

import pickle

import pytest

from simplespider import BaseObject


class MyObject(BaseObject):
    pass


@pytest.fixture(params=[BaseObject, MyObject])
def obj_class(request):
    return request.param


def test_object_functionality(obj_class):
    obj = obj_class(hello='world')

    assert obj['hello'] == 'world'
    obj['spam'] = 'eggs'
    assert obj['spam'] == 'eggs'

    obj['hello'] = 'World'
    assert obj['hello'] == 'World'

    with pytest.raises(KeyError):
        obj['doesntexist']

    del obj['hello']
    with pytest.raises(KeyError):
        obj['hello']
    with pytest.raises(KeyError):
        del obj['hello']


def test_object_repr():
    assert repr(BaseObject(eggs=2, spam=1)) == \
        'simplespider.BaseObject(eggs=2, spam=1)'
    assert repr(MyObject(eggs=2, spam=1)) == \
        'simplespider.tests.test_simple_objects.MyObject(eggs=2, spam=1)'


def test_object_pickling(obj_class):
    obj = obj_class(spam='SPAM', eggs='EGGS', bacon='BACON')
    assert obj['spam'] == 'SPAM'
    assert obj['eggs'] == 'EGGS'
    assert obj['bacon'] == 'BACON'

    pickled = pickle.dumps(obj)
    unpickled = pickle.loads(pickled)
    assert unpickled['spam'] == 'SPAM'
    assert unpickled['eggs'] == 'EGGS'
    assert unpickled['bacon'] == 'BACON'
