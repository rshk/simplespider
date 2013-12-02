"""
Tests for the new task-runner based storage.
"""

import anydbm
import json

import pytest

from simplespider.storage import StoreObjectTask, DictStorage, AnydbmStorage


def test_store_object_task():
    task = StoreObjectTask(data={'one': '1', 'two': '2'})
    assert task['data']['one'] == '1'
    assert task['data']['two'] == '2'

    with pytest.raises(TypeError):
        StoreObjectTask()
    with pytest.raises(TypeError):
        StoreObjectTask(foo='bar')


def test_dict_storage():
    storage = DictStorage()

    assert storage.match(StoreObjectTask(data={'foo': 'bar'}))
    assert not storage.match(None)
    assert not storage.match({})

    storage(StoreObjectTask(data={'name': 'Cat', '_type': 'pet'}))
    storage(StoreObjectTask(data={'name': 'Dog', '_type': 'pet'}))
    storage(StoreObjectTask(data={'name': 'Chicken', '_type': 'food'}))
    storage(StoreObjectTask(data={'name': 'Cow', '_type': 'food'}))

    assert storage._storage['pet'] == [
        {'name': 'Cat', '_type': 'pet'},
        {'name': 'Dog', '_type': 'pet'}]
    assert storage._storage['food'] == [
        {'name': 'Chicken', '_type': 'food'},
        {'name': 'Cow', '_type': 'food'}]


def test_anydbm_storage(tmpdir):
    dbfile = str(tmpdir.join('mydata.db'))
    storage = AnydbmStorage(path=dbfile)

    assert storage.match(StoreObjectTask(data={'foo': 'bar'}))
    assert not storage.match(None)
    assert not storage.match({})

    cat = {'name': 'Cat', '_type': 'pet', '_id': 'cat'}
    dog = {'name': 'Dog', '_type': 'pet', '_id': 'dog'}
    chicken = {'name': 'Chicken', '_type': 'food', '_id': 'chicken'}
    cow = {'name': 'Cow', '_type': 'food', '_id': 'cow'}

    storage(StoreObjectTask(data=cat))
    storage(StoreObjectTask(data=dog))
    storage(StoreObjectTask(data=chicken))
    storage(StoreObjectTask(data=cow))

    del storage  # Should sync during GC

    db = anydbm.open(dbfile, 'r')
    assert json.loads(db['pet.cat']) == cat
    assert json.loads(db['pet.dog']) == dog
    assert json.loads(db['food.chicken']) == chicken
    assert json.loads(db['food.cow']) == cow


def test_anydbm_storage_exc():
    with pytest.raises(TypeError):
        AnydbmStorage()
