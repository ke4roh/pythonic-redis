from .collections import ObjectRedis, RedisDict, \
     RedisList, RedisSet, RedisSortedSet
from .ttl import RedisTTLSet, RedisTime

__version__ = '0.8.0'
VERSION = tuple(map(int, __version__.split('.')))
