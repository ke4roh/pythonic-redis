# -*- coding: utf-8 -*-
import pytest
from pyredis import RedisTTLSet
import pickle

__author__ = 'ke4roh'


class TestRedisTTLSet(object):
    def test_set(self, sr):
        t = 1
        s = RedisTTLSet('foo', 5, redis=sr, time=lambda: t)
        s.add('grunge')
        assert ('grunge' in s)
        t = 2
        s.add(True)
        t = 3
        s.add(('graph'))
        assert set(['grunge', True, ('graph')]) == set(s)
        assert 3 == len(s)
        t = 6
        assert 2 == sum(1 for _ in s)
        assert set([True, ('graph')]) == set(s)

        with pytest.raises(TypeError):
            s.add(['nohash'])

        s.clear()
        assert 0 == len(s)

    def test_len_cleanup(self, sr):
        t = 1
        s = RedisTTLSet('foo', 5, redis=sr, time=lambda: t)
        s.add('grunge')
        t = 2
        s.add('oscar')
        t = 3
        s.add('abby')
        assert 3 == len(s)

        # Watch actual size in storage decrease as an element is removed for
        # expiration
        assert 3 == sr.zcard('foo')
        t = 6.1
        assert 3 == sr.zcard('foo')
        assert 2 == len(s)
        assert 2 == sr.zcard('foo')
        t = 10
        assert 0 == len(s)
        assert 0 == sr.zcard('foo')

    def test_copy_cleanup(self, sr):
        t = 1
        s = RedisTTLSet('foo', 5, redis=sr, time=lambda: t)
        s.add('grunge')
        t = 2
        s.add('oscar')
        t = 3
        s.add('abby')
        assert set(['grunge', 'oscar', 'abby']) == set(s)
        assert 3 == len(s)

        # Watch actual size in storage decrease as an element is removed for
        # expiration
        assert 3 == sr.zcard('foo')
        t = 6.1
        assert 3 == sr.zcard('foo')
        assert set(['oscar', 'abby']) == set(s)
        assert 2 == sr.zcard('foo')
        t = 10
        assert set() == set(s)
        assert 0 == sr.zcard('foo')

        s.update(list(range(0, 50)))
        assert 50 == len(s)
        t = 16
        assert 0 == len(s)

    def test_repr(self, sr):
        s = RedisTTLSet('foo', 5, redis=sr)
        s.add('grunge')
        assert "<RedisTTLSet(name='foo',{'grunge'})>" == str(s)
