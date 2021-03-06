# -*- coding: utf-8 -*-
import collections
import redis
import pickle
import time
from .collections import ObjectRedis, RedisSortedSet, _repr
__author__ = 'ke4roh'


def _missing(x):
    raise KeyError(str(x))


class RedisTime(object):
    """
    A clock backed by Redis, used to save on round-trip calls to check database
    time for TTL sets.  This is particularly useful during iterating over an
    entire collection - the clock need not be fetched for each item.
    """

    def __init__(self, redis=None, refresh_interval=5):
        """

        :param redis: The redis connection to use
        :param refresh_interval: The time (in seconds) to allow between checks
        of the redis clock
        """
        self.refresh_interval = refresh_interval
        self.redis = redis
        self.delta = 0
        self.next_check = 0

    def time(self):
        if self.next_check < time.time():
            sec, micros = self.redis.time()
            rclock = sec + micros * 1e-6
            now = time.time()
            self.delta = rclock - now
            self.next_check = now + self.refresh_interval
            return rclock
        else:
            return self.delta + time.time()


class RedisTTLSet(collections.MutableSet):
    """
    A set, whose items expire after a specified time.
    """

    def __init__(self, name, ttl, redis=redis.StrictRedis(),
                 serializer=pickle, time=None):
        """

        :param name: The name of this collection - its key in Redis
        :param ttl: How long items stay in the set
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an
            object (to store) into a byte array, and
            "loads" to turn a byte array into an object.  Default = pickle
        :param time: a function to return the current time, default = use
            RedisTime.time to periodically check Redis for the official time
            and use the local clock to measure during intervals in-between
            those checks, thus saving some round-trip delays to consult the
            Redis clock, especially with fast iterations over the set
            (which have already discarded the expired members a priori) and
            the slow iterations which might need to discard
            other elements as they expire and before yielding them.
        """
        self.redis = redis
        self.name = name
        self.serializer = serializer
        self.ttl = ttl
        self.time = time or RedisTime(redis=redis).time
        self.dict = RedisSortedSet(name, redis=redis, serializer=serializer)

    def __iter__(self):
        """
        :return: An iterator over all the items.  Only items that were
          available at the initial call time will be returned.  Only
          non-expired items will be returned.
        """
        self.__cleanup()
        for k, v in self.dict.items():
            if v < self.time():
                # If the expiry time has passed, it might have been refreshed
                if self.__contains__(k):
                    yield k
            else:
                yield k

    def __contains__(self, item):
        """
        :return: True if the item is in the set and not expired, false
            otherwise
        """
        expiry = self.dict.get(item, None)
        if expiry is None:
            return False
        if expiry < self.time():
            self.discard(item)
            return False
        return True

    def __len__(self):
        """
        :return: The number of non-expired elements.  This will clear out any
        expired elements.  Time is O(log(N)+M),
        where M is the number of expired elements.
        """
        self.__cleanup()
        return len(self.dict)

    def __cleanup(self):
        """Remove expired elements. O(log(N) + M)"""
        self.redis.zremrangebyscore(self.name, float("-inf"), self.time())

    def update(self, *other):
        t = self.time() + self.ttl
        self.dict.update((dict((k, t) for k in
                          [item for sublist in other for item in sublist])))

    def add(self, item):
        self.dict[item] = self.time() + self.ttl

    def discard(self, item):
        try:
            self.dict.__delitem__(item)
        except KeyError:
            pass

    def clear(self):
        self.dict.clear()

    def __repr__(self):
        return _repr(self, box='{%s}')
