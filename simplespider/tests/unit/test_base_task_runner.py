from simplespider import BaseTaskRunner


def test_base_task_runner():
    btr = BaseTaskRunner(foo='bar', spam='eggs')

    assert btr.conf == dict(foo='bar', spam='eggs')
    assert btr.match({}) is True
    assert btr({}) is None
