# -*- coding: utf-8 -*-
from __future__ import absolute_import
from redis import StrictRedis
import pickle
from collections import MutableMapping, MutableSequence, MutableSet
from ._compat import iteritems, OrderedDict
import string
import random

__author__ = 'ke4roh'


class ObjectRedis(MutableMapping):
    """
    A Dictionary view of a Redis database, supporting object keys and arbitrary
    object values.  This implementation uses the Redis key namespace as the
    namespace for its keys.

    If collections are stored in Redis with the supported collection types, the
    basic python-wrapped collections will be returned, using the same
    serlalizers provided (default is pickle).
    """

    def __init__(self, redis=None, namespace=None, serializer=pickle,
                 key_serializer=pickle, missing=None, missing_ttl=None,
                 *args, **kwargs):
        """
        :param redis: The StrictRedis connection to use
        :param namespace: Prepended to keys, None to prepend nothing.  If
              namespace is none, then all contents of the database are
              considered members of the collection and processed through the
              serializers.  The namespace will be encoded as bytes
              (str(ns).encode('utf-8')) if it's not already a byte array
        :param serializer: An object containing functions "dumps" to turn an
             object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        :param key_serializer: Like serializer, but applied to keys
        :param missing: A function to return values that were missing from the
             collection (default = raise KeyError)
        :param missing_ttl: A function to return the TTL for values inserted
             by the missing call (default is None - i.e. no expiration)
        """
        self.redis = redis or StrictRedis(*args, **kwargs)
        self.namespace = None
        if namespace is not None:
            self.namespace = ((type(namespace) is bytes and namespace) or
                              str(namespace).encode('utf-8')) + b":::"
        self.serializer = serializer
        self.key_serializer = key_serializer
        if missing is not None:
            self.__missing__ = missing
        if missing_ttl is not None:
            self.__missing_ttl__ = missing_ttl

    def __getitem__(self, key):
        """
        Get an item from the collection in constant time O(1)
        :param key: The key to find (any object)
        :return: The value at that key
        """
        rkey = self._ns(key)
        rtype = self.redis.type(rkey)
        if rtype == b'none':
            return self.__memoize(key)
        elif rtype == b'string':
            bval = self.redis.get(rkey)
            if bval is not None:
                return self.serializer.loads(bval)
            return self.__memoize(key)  # It went away between typing and getting
        elif rtype == b'list':
            return RedisList(rkey, self.redis, self.serializer)
        elif rtype == b'set':
            return RedisSet(rkey, self.redis, self.serializer)
        elif rtype == b'hash':
            return RedisDict(rkey, self.redis,
                             self.serializer, self.key_serializer)
        elif rtype == b'zset':
            return RedisSortedSet(rkey, self.redis, self.serializer)
        else:
            raise NotImplementedError(str(rtype))

    def __memoize(self, key):
        val = self.__missing__(key)
        self.set(key, val, self.__missing_ttl__(key))
        return val

    def __missing__(self, key):
        """Return the value corresponding to the key if possible"""
        raise KeyError(str(key))

    def __missing_ttl__(self, key):
        """Return the TTL for the given key"""
        return None

    def __setitem__(self, key, value):
        self.set(key, value)

    def set(self, key, value, ttl=None):
        """
        Set an item in the collection, constant time O(1)
        :param key: the key to set (within the namespace of this ObjectRedis)
        :param value: the value to set
        :param ttl: the duration, in seconds, this value should live
        :return: None
        """
        key.__hash__()
        bkey = self._ns(key)
        d = dir(value)
        if "__imul__" in d and "__iter__" in d:  # list
            def new_list(pipe):
                rl = RedisList(bkey, pipe, self.serializer)
                rl.clear()
                rl.extend(value)
                if ttl is not None:
                    pipe.expire(bkey, ttl)

            self.redis.transaction(new_list, key)
        elif "__xor__" in d and "__iter__" in d:  # set
            def new_set(pipe):
                rs = RedisSet(bkey, pipe, self.serializer)
                rs.clear()
                rs.update(value)
                if ttl is not None:
                    pipe.expire(bkey, ttl)

            self.redis.transaction(new_set, key)
        elif "__getitem__" in d and 'index' not in d:  # hash
            def new_hash(pipe):
                rd = RedisDict(bkey, pipe, self.serializer, self.serializer)
                rd.clear()
                rd.update(value)
                if ttl is not None:
                    pipe.expire(bkey, ttl)

            self.redis.transaction(new_hash, key)
        elif "__getitem__" in d and 'values' in d:  # zset
            def new_zset(pipe):
                rz = RedisSortedSet(bkey, pipe, self.serializer)
                rz.clear()
                rz.update(value)
                if ttl is not None:
                    pipe.expire(bkey, ttl)

            self.redis.transaction(new_zset, key)
        else:  # string (other)
            if ttl is None:
                self.redis.set(name=bkey, value=self.serializer.dumps(value))
            else:
                self.redis.setex(name=bkey, time=ttl, value=self.serializer.dumps(value))

    def __contains__(self, key):
        """
        O(1)
        :return: True if the key exists in Redis, false otherwise
        """
        return self.redis.exists(self._ns(key))

    def __delitem__(self, key):
        """
        Remove an item from the collection O(1)
        :raises KeyError if the key is not in the collection
        """
        if self.redis.delete(self._ns(key)) == 0:
            raise KeyError(str(key))

    def __iter__(self):
        """
        Return an iterator over the keys in this object.  Time is proportional
        to the number of keys in this namespace.
        """
        for k in self.redis.scan_iter(
                match=((self.namespace is not None and
                                self.namespace + b'*') or None)):
            try:
                # _dns can't be done in a list comprehension because the
                # exceptions need to be handled in the case of a null namespace
                # and traversing other items, or in case of different pickling
                # schemes, different namespace termination levels ("foo:" and
                # "foo:bar:", etc.).
                yield self._dns(k)
            except Exception:  # Other namespaces won't match
                pass

    def __len__(self):
        """Time is proportional to the number of keys in this namespace. O(N)
        :return number of items in this namespace
        """
        return sum(1 for _ in self.__iter__())

    def _dns(self, key):
        """
        decode a stored key by removing the namespace and deserializing it
        :param key: the key as stored in redis with the namespace
        :return: the object key,
        :raises ValueError if the namespace doesn't match
        """
        if self.namespace is not None:
            if key.startswith(self.namespace):
                bkey = key[len(self.namespace):]
            else:
                raise ValueError("mismatched namespace: " + str(key))
        else:
            bkey = key
        return self.key_serializer.loads(bkey)

    def _ns(self, key):
        """
        Convert an object key into one with the redis namespace and a
        serialized version of the suffix
        :param key: any object
        :return: a redis key beginning with the namepsace for this object,
            followed by the serialized object
        """
        if self.namespace is not None:
            return self.namespace + self.key_serializer.dumps(key)
        else:
            return self.key_serializer.dumps(key)

    def __repr__(self):
        return _repr(self, '{%s}', meta='namespace')


def _repr(obj, box=None, meta="name"):
    """
    Generate string representations of collections in constant time and
    reasonable screen real-estate by representing only the first few items
    of the collection if there are many.

    :param obj: The object (a collection) to represent
    :param box: %s enclosed in the appropriate brackets for this collection,
       default is "[%s]" for non-dicts, and "{%s}" for dicts.
    :param meta: The field to display (default "name" or "namespace"
    (or something else?))
    :return: A string representation of the object
    """
    l = []
    if 'items' in dir(obj):
        iter = obj.items
        box = box or '{%s}'

        def one(x):
            return repr(x[0]) + ': ' + repr(x[1])
    else:
        iter = obj.__iter__
        box = box or '[%s]'

        def one(x):
            return repr(x)

    for i in iter():
        if len(l) >= 10:
            l.append('…')
            break
        l.append(one(i))
    return ('<%s(%s=%r,' + box + ')>') % \
           (obj.__class__.__name__, meta, obj.__dict__.get(meta), ', '.join(l))


_TOKEN_CHARS = (string.ascii_letters + string.digits)


def _token():
    return ''.join(random.choice(_TOKEN_CHARS) for _ in range(32)). \
        encode("utf8")


def _dict_eq(a, b):
    """
    Compare dictionaries using their items iterators and loading as much
    as half of each into a local temporary store.  For comparisons of ordered
    dicts, memory usage is nil.  For comparisons of dicts whose iterators
    differ in sequence maximally, memory consumption is O(N).  Execution time
    is O(N).

    :param a: one dict
    :param b: another dict
    :return: True if they're the same, false otherwise
    """
    # The memory consumption here is to make a linear improvement in execution
    # time.  In the case of a dict backed by Redis, it is faster to iterate
    # over N items than to retrieve each one, by a factor of 10 or more
    # because of the reduced round-trips to the server.
    size = len(a)
    if size != len(b):
        return False

    # Iterate over both dicts.  Compare items.  If the same ones come up
    # at the same time, great, they match.  If different ones come up,
    # store them in the am and bm collections of misses.  Check for prior
    # misses that may be matched by the new elements.
    bi = iteritems(b)
    am = {}
    bm = {}
    for ak, av in iteritems(a):
        bk, bv = next(bi)
        if ak == bk:
            if av != bv:
                return False
        else:  # keys differ
            if ak in bm:
                if bm[ak] == av:
                    del bm[ak]
                else:
                    return False
            else:
                am[ak] = av
            if bk in am:
                if am[bk] == bv:
                    del am[bk]
                else:
                    return False
            else:
                bm[bk] = bv
        if len(am) + len(bm) > size:
            return False
    return len(am) + len(bm) == 0


class RedisList(MutableSequence):
    """A list backed by Redis, using the Redis linked list construct, and
    stored a single Redis value.
    Operations on the ends of the list, and len() are O(1).
    Operations on elements by index are O(N)."""

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an
             object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __getitem__(self, index):
        """
        O(N)
        :param index: The integer index of the thing to find, negative to start
            at the end.
        :return: The item at that index
        """
        rval = self.redis.lindex(self.name, index)
        if rval is None:
            raise IndexError("empty list")
        return self.serializer.loads(rval)

    def __setitem__(self, index, value):
        """
        O(N)
        :param index: The integer index of the thing to store, negative to
            start at the end.
        :param value: th thing to store
        """
        self.redis.lset(self.name, index, self.serializer.dumps(value))

    def __delitem__(self, index):
        """
        O(N)
        :param index: The index of item to remove, negative is from the end
        """
        token = b'-=-DELETING-=-' + _token()
        self.redis.pipeline().lset(self.name, index, token). \
            lrem(self.name, 1, token).execute()

    def __len__(self):
        """
        O(1)
        :return: The number of elements in the list
        """
        return self.redis.llen(self.name)

    def __insert(self, pipe, index, value):
        value = self.serializer.dumps(value)
        if index >= pipe.llen(self.name):
            pipe.rpush(self.name, value)
        else:
            current = pipe.lindex(self.name, index)
            token = b'-=-INSERTING-=-' + _token()
            pipe.lset(self.name, index, token)
            pipe.linsert(self.name, 'BEFORE', token, value)
            pipe.linsert(self.name, 'AFTER', token, current)
            pipe.lrem(self.name, 1, token)

    def insert(self, index, value):
        self.redis.transaction(lambda pipe: self.__insert(pipe, index, value),
                               self.name)

    def append(self, value):
        self.redis.rpush(self.name, self.serializer.dumps(value))

    def extend(self, values):
        self.redis.rpush(self.name,
                         *[self.serializer.dumps(v) for v in values])

    def clear(self):
        self.redis.delete(self.name)

    def remove(self, value):
        if not self.redis.lrem(self.name, 1, self.serializer.dumps(value)):
            raise ValueError()

    def __pop(self, pipe, index, rbox):
        bval = pipe.lindex(self.name, index)
        if bval is None:
            raise IndexError()

        rbox.append(bval)
        token = _token()
        pipe.lset(self.name, index, token)
        pipe.lrem(self.name, 1, token)

    def pop(self, index=-1):
        if index == -1:
            rval = self.redis.rpop(self.name)
        elif index == 0:
            rval = self.redis.lpop(self.name)
        else:
            rbox = []
            self.redis.transaction(lambda pipe: self.__pop(pipe, index, rbox),
                                   self.name)
            if len(rbox):
                rval = rbox[-1]
            else:
                raise IndexError()
        if rval is None:
            raise IndexError()
        return self.serializer.loads(rval)

    def __iter__(self):
        for x in self.redis.lrange(self.name, 0, self.__len__()):
            yield self.serializer.loads(x)

    def __contains__(self, item):
        try:
            return self.index(item) is not None and True
        except ValueError:
            return False

    def __reversed__(self):
        """
        Load the list in memory and iterate over it backwards.  O(N)
        Iterating backwards without loading the list would take O(N*N).
        """
        return list(self).__reversed__()

    def __index(self, pipe, value, start, stop, rbox):
        if stop is None:
            stop = pipe.llen(self.name)
        bi = self.serializer.dumps(value)
        ix = 0
        for x in pipe.lrange(self.name, start, stop):
            if x == bi:
                rbox.append(ix)
                return
            ix += 1

    def index(self, value, start=0, stop=None):
        rbox = []
        self.redis.transaction(lambda pipe:
                               self.__index(pipe, value, start, stop, rbox),
                               self.name)
        if len(rbox):
            return rbox[-1]
        else:
            raise ValueError()

    def __repr__(self):
        return _repr(self)


class RedisSet(MutableSet):
    """
    A set, backed by the Redis set construct.
    """

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an
             object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __iter__(self):
        for item in self.redis.sscan_iter(self.name):
            yield self.serializer.loads(item)

    def __len__(self):
        return self.redis.scard(self.name)

    def __contains__(self, item):
        return self.redis.sismember(self.name, self.serializer.dumps(item))

    def update(self, *others):
        # The call to __hash__ for each item insures it's hashable (i.e.
        # unmodifiable), and thus suitable for a set.
        # These sets are persisted and unmodifiable once they're saved, but
        # I haven't thought of a good reason to change the basic contract
        # for set - because this set will also be transformed into an
        # in-memory set in some cases.
        new_data = [(item.__hash__() or True) and self.serializer.dumps(item)
                    for sublist in others for item in sublist]
        if len(new_data):
            self.redis.sadd(self.name, *new_data)

    def add(self, item):
        """
        :param item: One or more items to be added
        :return: the number of things added
        """
        self.update((item,))

    def discard(self, item):
        self.redis.srem(self.name, self.serializer.dumps(item))

    def clear(self):
        self.redis.delete(self.name)

    def __repr__(self):
        return _repr(self, '{%s}')


class RedisDict(MutableMapping):
    """
    A dictionary, backed by Redis
    """

    def __init__(self, name, redis=StrictRedis(), serializer=pickle,
                 key_serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an
             object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        :param key_serializer: Like serializer, but applied to keys
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer
        self.key_serializer = key_serializer

    def __getitem__(self, item):
        val = self.redis.hget(self.name, self.key_serializer.dumps(item))
        if val is None:
            raise KeyError()
        return self.serializer.loads(val)

    def __setitem__(self, item, value):
        item.__hash__()  # raise a TypeError if it isn't immutable
        self.redis.hset(self.name, self.key_serializer.dumps(item),
                        self.serializer.dumps(value))

    def __delitem__(self, item):
        if not self.redis.hdel(self.name, self.key_serializer.dumps(item)):
            raise KeyError()

    def __iter__(self):
        for k, v in self.redis.hscan_iter(self.name):
            yield self.key_serializer.loads(k)

    def __len__(self):
        return self.redis.hlen(self.name)

    def clear(self):
        self.redis.delete(self.name)

    def __repr__(self):
        return _repr(self, '{%s}')

    def items(self):
        for k, v in self.redis.hscan_iter(self.name):
            yield self.key_serializer.loads(k), self.serializer.loads(v)

    def iteritems(self):
        return self.items()

    def __eq__(self, other):
        """
        :return contents equal to the other dict
        """
        return _dict_eq(self, other)


class RedisSortedSet(MutableMapping):
    """
    A Redis sorted set wrapped as a dict.  Entries are stored in the dictionary
    keys, scores are their values. Items are sorted in order by their values.
    Values must be floating point numbers.
    """

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The name of this set in Redis
        :param redis: The StrictRedis you want to use
        :param serializer: An object containing functions "dumps" to turn an
             object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __contains__(self, item):
        """Test to see if a key is in the set. O(1)"""
        return self.redis.zscore(self.name, self.serializer.dumps(item)) \
               is not None

    def items(self):
        """Return a generator over the keys and their sort values"""
        for k, v in self.redis.zscan_iter(self.name):
            yield self.serializer.loads(k), v

    def iteritems(self):
        return self.items()

    def __iter__(self):
        """Iterate over the keys, in order. O(N)"""
        for k, v in self.redis.zscan_iter(self.name):
            yield self.serializer.loads(k)

    def __len__(self):
        """Get the size of the set. O(1)"""
        return self.redis.zcard(self.name)

    def __getitem__(self, key):
        """Get the score of an item in the set. O(log N)"""
        rval = self.redis.zscore(self.name, self.serializer.dumps(key))
        if rval is None:
            raise KeyError(str(key))
        return rval

    def __setitem__(self, key, value):
        """Put an item in the set. O(log N)"""
        key.__hash__()  # See that it's hashable, otherwise it's not a key
        self.redis.zadd(self.name, value + 0.0, self.serializer.dumps(key))

    def __eq__(self, other):
        """
        Comparison to another RedisSortedSet is memory-efficient.  Comparison
        to other dictionaries takes memory and time O(N).
        """
        return _dict_eq(self, other)

    def index(self, value):
        """Return the rank of the value (its ordinal position in the set).
        O(log N)"""
        # This signature doesn't match the one from collections.Sequence
        # because specifying range is nonsense
        rval = self.redis.zrank(self.name, self.serializer.dumps(value))
        if rval is not None:
            return rval
        else:
            raise ValueError()

    def __delitem__(self, value):
        if self.redis.zrem(self.name, self.serializer.dumps(value)) == 0:
            raise KeyError()

    def update(*args, **kwds):
        new_stuff = {}
        self = args[0]
        args = args[1:]
        new_stuff.update(*args, **kwds)
        self.redis.zadd(self.name,
                        *[i for sub in [(v + 0, self.serializer.dumps(k))
                                        for k, v in iteritems(new_stuff)]
                          for i in sub])

    def clear(self):
        self.redis.delete(self.name)

    def __repr__(self):
        return _repr(self, '{%s}')
