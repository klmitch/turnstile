import redis

from turnstile import control
from turnstile import database

import tests


class PipelineContext(object):
    def __init__(self, db):
        self._db = db

    def __enter__(self):
        return self._db

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._db._watching = set()


class PubSub(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._messages = []
        self._subscriptions = set()

    def subscribe(self, channel):
        self._subscriptions.add(channel)

    def listen(self):
        for msg in self._messages:
            yield msg


class FakeDatabase(database.TurnstileRedis):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._actions = []
        self._fakedb = {}
        self._expireat = {}
        self._watcherror = {}
        self._watching = set()
        self._messages = []
        self._pubsub = None
        self._published = []

    def pipeline(self):
        self._actions.append(('pipeline',))
        return PipelineContext(self)

    def watch(self, key):
        self._actions.append(('watch', key))
        self._watching.add(key)

    def get(self, key):
        self._actions.append(('get', key))
        return self._fakedb.get(key)

    def multi(self):
        self._actions.append(('multi',))

    def set(self, key, value):
        self._actions.append(('set', key, value))
        self._fakedb[key] = value

    def expireat(self, key, expire):
        self._actions.append(('expireat', key, expire))
        self._expireat[key] = expire

    def execute(self):
        self._actions.append(('execute',))

        for watch in self._watching:
            count = self._watcherror.get(watch, 0)
            if count:
                self._watcherror[watch] = count - 1
                raise redis.WatchError()

    def pubsub(self, *args, **kwargs):
        self._actions.append(('pubsub', args, kwargs))
        self._pubsub = PubSub(*args, **kwargs)
        self._pubsub._messages = self._messages
        return self._pubsub

    def publish(self, channel, msg):
        self._actions.append(('publish', channel, msg))
        self._published.append((channel, msg))

    def zadd(self, key, *args):
        additions = zip(args[::2], args[1::2])
        self._actions.append(('zadd', key, additions))
        zset = self._fakedb.setdefault(key, [])
        for score, item in additions:
            idx = self._zfind(zset, item)
            if idx is None:
                zset.append((score, item))
            else:
                zset[idx] = (score, item)
        zset.sort(key=lambda x: x[0])

    def zrem(self, key, *values):
        self._actions.append(('zrem', key, values))
        zset = self._fakedb.setdefault(key, [])
        removals = []
        for val in values:
            idx = self._zfind(zset, val)
            if idx is not None:
                removals.append(idx)
        for idx in sorted(removals, reverse=True):
            del zset[idx]

    def _zfind(self, zset, value):
        for idx, (score, item) in enumerate(zset):
            if item == value:
                return idx
        return None

    def zrange(self, key, start, stop):
        self._actions.append(('zrange', key, start, stop))
        if key in self._fakedb:
            return [item[1] for item in self._fakedb[key]]
        else:
            return []

    def sadd(self, key, value):
        self._actions.append(('sadd', key, value))
        self._fakedb[key].add(value)

    def execute_command(self, *args, **kwargs):
        self._actions.append(('execute_command', args[0], args[1:], kwargs))
        raise Exception("Unhandled command %s" % args[0])


class FakeLimit(tests.GenericFakeClass):
    @classmethod
    def hydrate(cls, db, limit):
        return cls(db, **limit)

    def dehydrate(self):
        return self.kwargs

    def _route(self, mapper):
        mapper.routes.append(self)


class FakeLimitData(object):
    def __init__(self, limits=[]):
        self.limit_data = limits[:]
        self.limit_sum = len(limits)

    def set_limits(self, limits):
        if not limits:
            raise Exception("Fake-out failure")
        self.limit_data = limits[:]
        self.limit_sum = len(limits)

    def get_limits(self, limit_sum=None):
        if limit_sum and self.limit_sum == limit_sum:
            raise control.NoChangeException()
        elif self.limit_data and isinstance(self.limit_data[0], Exception):
            raise self.limit_data[0]
        return (self.limit_sum, self.limit_data)
