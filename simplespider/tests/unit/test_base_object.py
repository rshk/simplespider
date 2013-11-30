from simplespider import BaseObject


class MyObject(BaseObject):
    pass


def test_base_object():
    ob = BaseObject(foo='bar')
    assert repr(ob) == "simplespider.BaseObject(foo='bar')"

    assert isinstance(ob, dict)  # so we can trust it will work..

    ob2 = MyObject(foo='bar')
    assert repr(ob2) == "simplespider.tests.unit.test_base_object."\
        "MyObject(foo='bar')"


def test_base_object_pickle_serializable():
    import pickle
    ob = BaseObject(foo='bar')
    serialized = pickle.dumps(ob)
    ob2 = pickle.loads(serialized)
    assert ob == ob2
