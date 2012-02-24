import json
import random

import eventlet
import msgpack
import redis
import routes

from turnstile import database
from turnstile import limits

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


class ControlDaemonTest(database.ControlDaemon):
    def __init__(self, *args, **kwargs):
        super(ControlDaemonTest, self).__init__(*args, **kwargs)

        self._commands = []

    def _start(self):
        pass

    def _internal(self, *args):
        self._commands.append(('internal', args))

    def test(self, arg):
        self._commands.append(('test', arg))

    def failure(self, *args):
        self._commands.append(('failure', args))
        raise Exception("Failure")


class FakeParser(tests.GenericFakeClass):
    pass


class FakeConnection(object):
    pass


class FakeConnectionPool(tests.GenericFakeClass):
    pass


class FakeLimit(tests.GenericFakeClass):
    @classmethod
    def hydrate(cls, db, limit):
        return cls(db, **limit)

    def dehydrate(self):
        return self.kwargs

    def _route(self, mapper):
        mapper.routes.append(self)


class FakeMapper(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeMapper, self).__init__(*args, **kwargs)

        self.routes = []


class FakeFailingMapper(FakeMapper):
    def __init__(self, *args, **kwargs):
        raise Exception("Fake-out")


class ClassTest(object):
    def __init__(self, db, arg1, arg2, a=1, b=2, c=3, d=4, **kwargs):
        self.db = db
        self.args = (arg1, arg2)
        self.a = a
        self.b = b
        self.c = c
        self.d = d

        if 'expire' in kwargs:
            self.expire = kwargs['expire']

        self.hydrated = False

    @classmethod
    def hydrate(cls, db, obj_dict, arg1, arg2):
        obj = cls(db, arg1, arg2, **obj_dict)
        obj.hydrated = True
        return obj

    def dehydrate(self):
        result = {}
        for attr in 'abcd':
            result[attr] = getattr(self, attr)

        if hasattr(self, 'expire'):
            result['expire'] = self.expire

        return result


class TestSafeUpdate(tests.TestCase):
    def setUp(self):
        super(TestSafeUpdate, self).setUp()

        self.callobj = []

        self.stubs.Set(msgpack, 'loads', lambda x: x)
        self.stubs.Set(msgpack, 'dumps', lambda x: x)

    def _update(self, obj):
        self.assertIsInstance(obj, ClassTest)
        self.callobj.append(obj)
        return obj

    def test_make_object(self):
        db = FakeDatabase()
        result = db.safe_update('spam', ClassTest, self._update,
                                'arg1', 'arg2')

        self.assertEqual(len(self.callobj), 1)
        self.assertEqual(self.callobj[-1], result)
        self.assertEqual(result.db, db)
        self.assertEqual(result.args, ('arg1', 'arg2'))
        self.assertEqual(result.a, 1)
        self.assertEqual(result.b, 2)
        self.assertEqual(result.c, 3)
        self.assertEqual(result.d, 4)
        self.assertEqual(result.hydrated, False)
        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'spam'),
                ('get', 'spam'),
                ('multi',),
                ('set', 'spam', dict(a=1, b=2, c=3, d=4)),
                ('execute',),
                ])
        self.assertEqual(db._fakedb, dict(spam=dict(a=1, b=2, c=3, d=4)))
        self.assertEqual(db._expireat, {})

    def test_rehydrate_object(self):
        db = FakeDatabase()
        db._fakedb['spam'] = dict(a=4, b=3, c=2, d=1)
        result = db.safe_update('spam', ClassTest, self._update,
                                'arg1', 'arg2')

        self.assertEqual(len(self.callobj), 1)
        self.assertEqual(self.callobj[-1], result)
        self.assertEqual(result.db, db)
        self.assertEqual(result.args, ('arg1', 'arg2'))
        self.assertEqual(result.a, 4)
        self.assertEqual(result.b, 3)
        self.assertEqual(result.c, 2)
        self.assertEqual(result.d, 1)
        self.assertEqual(result.hydrated, True)
        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'spam'),
                ('get', 'spam'),
                ('multi',),
                ('set', 'spam', dict(a=4, b=3, c=2, d=1)),
                ('execute',),
                ])
        self.assertEqual(db._fakedb, dict(spam=dict(a=4, b=3, c=2, d=1)))
        self.assertEqual(db._expireat, {})

    def test_watch_error(self):
        db = FakeDatabase()
        db._watcherror['spam'] = 1
        result = db.safe_update('spam', ClassTest, self._update,
                                'arg1', 'arg2')

        self.assertEqual(len(self.callobj), 2)
        self.assertEqual(self.callobj[-1], result)
        self.assertEqual(result.db, db)
        self.assertEqual(result.args, ('arg1', 'arg2'))
        self.assertEqual(result.a, 1)
        self.assertEqual(result.b, 2)
        self.assertEqual(result.c, 3)
        self.assertEqual(result.d, 4)
        self.assertEqual(result.hydrated, True)
        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'spam'),
                ('get', 'spam'),
                ('multi',),
                ('set', 'spam', dict(a=1, b=2, c=3, d=4)),
                ('execute',),
                ('watch', 'spam'),
                ('get', 'spam'),
                ('multi',),
                ('set', 'spam', dict(a=1, b=2, c=3, d=4)),
                ('execute',),
                ])
        self.assertEqual(db._fakedb, dict(spam=dict(a=1, b=2, c=3, d=4)))
        self.assertEqual(db._expireat, {})

    def test_expiring_object(self):
        db = FakeDatabase()
        db._fakedb['spam'] = dict(a=4, b=3, c=2, d=1, expire=1000000.0)
        result = db.safe_update('spam', ClassTest, self._update,
                                'arg1', 'arg2')

        self.assertEqual(len(self.callobj), 1)
        self.assertEqual(self.callobj[-1], result)
        self.assertEqual(result.db, db)
        self.assertEqual(result.args, ('arg1', 'arg2'))
        self.assertEqual(result.a, 4)
        self.assertEqual(result.b, 3)
        self.assertEqual(result.c, 2)
        self.assertEqual(result.d, 1)
        self.assertEqual(result.expire, 1000000.0)
        self.assertEqual(result.hydrated, True)
        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'spam'),
                ('get', 'spam'),
                ('multi',),
                ('set', 'spam', dict(a=4, b=3, c=2, d=1, expire=1000000.0)),
                ('expireat', 'spam', 1000000.0),
                ('execute',),
                ])
        self.assertEqual(db._fakedb,
                         dict(spam=dict(a=4, b=3, c=2, d=1, expire=1000000.0)))
        self.assertEqual(db._expireat, dict(spam=1000000.0))


class TestLimitUpdate(tests.TestCase):
    def setUp(self):
        super(TestLimitUpdate, self).setUp()

        self.stubs.Set(msgpack, 'loads', json.loads)
        self.stubs.Set(msgpack, 'dumps', json.dumps)

    def test_limit_update_empty(self):
        db = FakeDatabase()
        limits = [
            FakeLimit(name='limit1'),
            FakeLimit(name='limit2'),
            FakeLimit(name='limit3'),
            ]
        db.limit_update('limits', limits)

        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'limits'),
                ('zrange', 'limits', 0, -1),
                ('multi',),
                ('zadd', 'limits', [(10, '{"name": "limit1"}')]),
                ('zadd', 'limits', [(20, '{"name": "limit2"}')]),
                ('zadd', 'limits', [(30, '{"name": "limit3"}')]),
                ('execute',),
                ])
        self.assertEqual(db._fakedb['limits'], [
                (10, '{"name": "limit1"}'),
                (20, '{"name": "limit2"}'),
                (30, '{"name": "limit3"}'),
                ])

    def test_limit_update_delete(self):
        db = FakeDatabase()
        db._fakedb['limits'] = [
            (10, '{"name": "limit1"}'),
            (20, '{"name": "limit2"}'),
            (30, '{"name": "limit3"}'),
            ]
        db.limit_update('limits', [])

        self.assertEqual(db._actions[:4], [
                ('pipeline',),
                ('watch', 'limits'),
                ('zrange', 'limits', 0, -1),
                ('multi',),
                ])
        self.assertEqual(db._actions[-1], ('execute',))

        tmp = sorted(list(db._actions[4:-1]), key=lambda x: x[2][0])
        self.assertEqual(tmp, [
                ('zrem', 'limits', ('{"name": "limit1"}',)),
                ('zrem', 'limits', ('{"name": "limit2"}',)),
                ('zrem', 'limits', ('{"name": "limit3"}',)),
                ])

        self.assertEqual(db._fakedb['limits'], [])

    def test_limit_update_overlap(self):
        db = FakeDatabase()
        db._fakedb['limits'] = [
            (10, '{"name": "limit1"}'),
            (20, '{"name": "limit3"}'),
            (30, '{"name": "limit4"}'),
            ]
        limits = [
            FakeLimit(name='limit1'),
            FakeLimit(name='limit2'),
            FakeLimit(name='limit3'),
            ]
        db.limit_update('limits', limits)

        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'limits'),
                ('zrange', 'limits', 0, -1),
                ('multi',),
                ('zrem', 'limits', ('{"name": "limit4"}',)),
                ('zadd', 'limits', [(10, '{"name": "limit1"}')]),
                ('zadd', 'limits', [(20, '{"name": "limit2"}')]),
                ('zadd', 'limits', [(30, '{"name": "limit3"}')]),
                ('execute',),
                ])
        self.assertEqual(db._fakedb['limits'], [
                (10, '{"name": "limit1"}'),
                (20, '{"name": "limit2"}'),
                (30, '{"name": "limit3"}'),
                ])

    def test_limit_update_watcherror(self):
        db = FakeDatabase()
        db._fakedb['limits'] = [
            (10, '{"name": "limit1"}'),
            (20, '{"name": "limit3"}'),
            (30, '{"name": "limit4"}'),
            ]
        db._watcherror['limits'] = 1
        limits = [
            FakeLimit(name='limit1'),
            FakeLimit(name='limit2'),
            FakeLimit(name='limit3'),
            ]
        db.limit_update('limits', limits)

        self.assertEqual(db._actions, [
                ('pipeline',),
                ('watch', 'limits'),
                ('zrange', 'limits', 0, -1),
                ('multi',),
                ('zrem', 'limits', ('{"name": "limit4"}',)),
                ('zadd', 'limits', [(10, '{"name": "limit1"}')]),
                ('zadd', 'limits', [(20, '{"name": "limit2"}')]),
                ('zadd', 'limits', [(30, '{"name": "limit3"}')]),
                ('execute',),
                ('watch', 'limits'),
                ('zrange', 'limits', 0, -1),
                ('multi',),
                ('zadd', 'limits', [(10, '{"name": "limit1"}')]),
                ('zadd', 'limits', [(20, '{"name": "limit2"}')]),
                ('zadd', 'limits', [(30, '{"name": "limit3"}')]),
                ('execute',),
                ])
        self.assertEqual(db._fakedb['limits'], [
                (10, '{"name": "limit1"}'),
                (20, '{"name": "limit2"}'),
                (30, '{"name": "limit3"}'),
                ])


class TestCommand(tests.TestCase):
    def test_command_basic(self):
        db = FakeDatabase()
        db.command('control', 'foo')

        self.assertEqual(db._published, [('control', 'foo')])

    def test_command_args(self):
        db = FakeDatabase()
        db.command('control', 'foo', 'bar', 1, 1.0)

        self.assertEqual(db._published, [('control', 'foo:bar:1:1.0')])


class TestInitialize(tests.TestCase):
    imports = {
        'FakeParser': FakeParser,
        'FakeConnection': FakeConnection,
        'FakeConnectionPool': FakeConnectionPool,
        }

    def setUp(self):
        super(TestInitialize, self).setUp()

        self.stubs.Set(database, 'TurnstileRedis', FakeDatabase)

    def test_missing_connection(self):
        with self.assertRaises(redis.ConnectionError):
            db = database.initialize({})

    def test_host_connection(self):
        config = dict(
            host='example.com',
            port='1234',
            db='5',
            password='password',
            socket_timeout='321',
            )
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(
                host='example.com', port=1234, db=5, password='password',
                socket_timeout=321))

    def test_host_minimal(self):
        config = dict(host='example.com')
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(host='example.com'))

    def test_unix_connection(self):
        config = dict(
            unix_socket_path='/tmp/redis',
            db='5',
            password='password',
            socket_timeout='321',
            )
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(
                unix_socket_path='/tmp/redis', db=5, password='password',
                socket_timeout=321))

    def test_unix_minimal(self):
        config = dict(unix_socket_path='/tmp/redis')
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(unix_socket_path='/tmp/redis'))

    def test_connection_pool_host(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              tests.GenericFakeClass)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=redis.Connection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_unix(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'unix_socket_path': '/tmp/redis',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              tests.GenericFakeClass)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                path='/tmp/redis',
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=redis.UnixDomainSocketConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_host_custom_class(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'connection_pool.connection_class': 'FakeConnection',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              tests.GenericFakeClass)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=FakeConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_unix_custom_class(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'unix_socket_path': '/tmp/redis',
            'connection_pool.connection_class': 'FakeConnection',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              tests.GenericFakeClass)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                unix_socket_path='/tmp/redis',
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=FakeConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_host_custom_pool(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'connection_pool': 'FakeConnectionPool',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              FakeConnectionPool)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=redis.Connection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_unix_custom_pool(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'unix_socket_path': '/tmp/redis',
            'connection_pool': 'FakeConnectionPool',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              FakeConnectionPool)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                path='/tmp/redis',
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=redis.UnixDomainSocketConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_host_custom_pool_custom_class(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'connection_pool': 'FakeConnectionPool',
            'connection_pool.connection_class': 'FakeConnection',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              FakeConnectionPool)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=FakeConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))

    def test_connection_pool_unix_custom_pool_custom_class(self):
        self.stubs.Set(redis, 'ConnectionPool', tests.GenericFakeClass)
        config = {
            'host': 'example.com',
            'port': '1234',
            'db': '5',
            'password': 'password',
            'socket_timeout': '321',
            'unix_socket_path': '/tmp/redis',
            'connection_pool': 'FakeConnectionPool',
            'connection_pool.connection_class': 'FakeConnection',
            'connection_pool.max_connections': '100',
            'connection_pool.parser_class': 'FakeParser',
            'connection_pool.arg1': 'arg1',
            'connection_pool.arg2': '2',
            }
        db = database.initialize(config)

        self.assertEqual(db._args, ())
        self.assertEqual(len(db._kwargs), 1)
        self.assertIn('connection_pool', db._kwargs)
        self.assertIsInstance(db._kwargs['connection_pool'],
                              FakeConnectionPool)

        conn_pool = db._kwargs['connection_pool']
        self.assertEqual(conn_pool.args, ())
        self.assertEqual(conn_pool.kwargs, dict(
                host='example.com',
                port=1234,
                unix_socket_path='/tmp/redis',
                db=5,
                password='password',
                socket_timeout=321,
                connection_class=FakeConnection,
                max_connections=100,
                parser_class=FakeParser,
                arg1='arg1',
                arg2='2',
                ))


class TestControlDaemon(tests.TestCase):
    def setUp(self):
        super(TestControlDaemon, self).setUp()

        # Turn off random number generation
        self.stubs.Set(random, 'random', lambda: 1.0)

    def stub_spawn(self, call=False):
        self.spawns = []

        def fake_spawn_n(method, *args, **kwargs):
            self.spawns.append(('spawn_n', method, args, kwargs))
            if call:
                return method(*args, **kwargs)

        def fake_spawn_after(delay_time, method, *args, **kwargs):
            self.spawns.append(('spawn_after', delay_time, method,
                                args, kwargs))
            if call:
                return method(*args, **kwargs)

        self.stubs.Set(eventlet, 'spawn_n', fake_spawn_n)
        self.stubs.Set(eventlet, 'spawn_after', fake_spawn_after)

    def stub_start(self):
        self.stubs.Set(database.ControlDaemon, '_start', lambda x: None)

    def stub_reload(self, mapper=FakeMapper):
        self.stubs.Set(msgpack, 'loads', lambda x: x)
        self.stubs.Set(limits, 'Limit', FakeLimit)
        self.stubs.Set(routes, 'Mapper', mapper)

    def test_init(self):
        self.stub_spawn(True)

        def fake_reload(obj):
            obj._reloaded = True

        self.stubs.Set(database.ControlDaemon, '_listen', lambda obj: 'listen')
        self.stubs.Set(database.ControlDaemon, '_reload', fake_reload)

        daemon = database.ControlDaemon('db', 'middleware', 'config')

        self.assertEqual(daemon._db, 'db')
        self.assertEqual(daemon._middleware, 'middleware')
        self.assertEqual(daemon._config, 'config')
        self.assertIsInstance(daemon._pending, eventlet.semaphore.Semaphore)
        self.assertEqual(daemon._listen_thread, 'listen')
        self.assertEqual(daemon._reloaded, True)

    def test_listen_basic(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (), {})])
        self.assertIsInstance(db._pubsub, PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, {})
        self.assertEqual(pubsub._subscriptions, set(['control']))

    def test_listen_shard(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware',
                                        dict(shard_hint='shard'))
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (),
                                        dict(shard_hint='shard'))])
        self.assertIsInstance(db._pubsub, PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, dict(shard_hint='shard'))
        self.assertEqual(pubsub._subscriptions, set(['control']))

    def test_listen_control(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware',
                                        dict(channel='spam'))
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (), {})])
        self.assertIsInstance(db._pubsub, PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, {})
        self.assertEqual(pubsub._subscriptions, set(['spam']))

    def test_listen_nonmessage(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='nosuch',
                pattern=None,
                channel='control',
                data='test:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_wrongchannel(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='wrongchannel',
                data='test:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_empty(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data=':foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_internal(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='_internal:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])
        self.assertEqual(self.log_messages, [
                "Cannot call internal method '_internal'",
                ])

    def test_listen_unknown(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='unknown:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])
        self.assertEqual(self.log_messages, [
                "No such command 'unknown'",
                ])

    def test_listen_uncallable(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='spam:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon.spam = 'maps'
        daemon._listen()

        self.assertEqual(daemon._commands, [])
        self.assertEqual(self.log_messages, [
                "Command 'spam' is not callable",
                ])

    def test_listen_badargs(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='test:arg1:arg2'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])
        self.assertEqual(len(self.log_messages), 1)
        self.assertTrue(self.log_messages[0].startswith(
                "Failed to execute command 'test' arguments "
                "['arg1', 'arg2']"))

    def test_listen_exception(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='failure:arg1:arg2'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [('failure', ('arg1', 'arg2'))])
        self.assertEqual(len(self.log_messages), 1)
        self.assertTrue(self.log_messages[0].startswith(
                "Failed to execute command 'failure' arguments "
                "['arg1', 'arg2']"))

    def test_listen_callout(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='test:arg'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [('test', 'arg')])
        self.assertEqual(self.log_messages, [])

    def test_listen_callout_alternate_channel(self):
        db = FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='alternate',
                data='test:arg'))
        daemon = ControlDaemonTest(db, 'middleware',
                                   dict(channel='alternate'))
        daemon._listen()

        self.assertEqual(daemon._commands, [('test', 'arg')])
        self.assertEqual(self.log_messages, [])

    def test_ping_nochan(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware', {})
        daemon.ping(None)

        self.assertEqual(db._published, [])

    def test_ping_basic(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware', {})
        daemon.ping('pong')

        self.assertEqual(db._published, [('pong', 'pong')])

    def test_ping_basic_node(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware',
                                        dict(node_name='node'))
        daemon.ping('pong')

        self.assertEqual(db._published, [('pong', 'pong:node')])

    def test_ping_data(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware', {})
        daemon.ping('pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong::data')])

    def test_ping_data_node(self):
        self.stub_start()

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware',
                                        dict(node_name='node'))
        daemon.ping('pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong:node:data')])

    def test_reload_command_noargs(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware', {})
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_noargs_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_noargs_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_badtype(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware', {})
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_badtype_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_badtype_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_immediate(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware', {})
        daemon.reload('immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_immediate_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware', {})
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_given(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware', {})
        daemon.reload('spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_bad_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread', '18.0.5')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_given_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = database.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon._reload, (), {})
                ])

    def test_reload_noacquire(self):
        self.stub_start()
        self.stub_reload()

        db = FakeDatabase()
        db._fakedb['limits'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = database.ControlDaemon(db, middleware, {})
        daemon._pending.acquire()
        daemon._reload()

        self.assertEqual(db._actions, [])
        self.assertFalse(hasattr(middleware, 'mapper'))

    def test_reload(self):
        self.stub_start()
        self.stub_reload()

        db = FakeDatabase()
        db._fakedb['limits'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = database.ControlDaemon(db, middleware, {})
        daemon._reload()

        self.assertEqual(db._actions, [('zrange', 'limits', 0, -1)])
        self.assertTrue(hasattr(middleware, 'mapper'))
        self.assertIsInstance(middleware.mapper, FakeMapper)
        self.assertEqual(middleware.mapper.kwargs, dict(register=False))
        self.assertEqual(len(middleware.mapper.routes), 2)
        for idx, route in enumerate(middleware.mapper.routes):
            self.assertIsInstance(route, FakeLimit)
            self.assertEqual(route.args, (db,))
            self.assertEqual(route.kwargs, dict(limit='limit%d' % (idx + 1)))
        self.assertEqual(daemon._pending.balance, 1)

    def test_reload_alternate(self):
        self.stub_start()
        self.stub_reload()

        db = FakeDatabase()
        db._fakedb['alternate'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = database.ControlDaemon(db, middleware,
                                        dict(limits_key='alternate'))
        daemon._reload()

        self.assertEqual(db._actions, [('zrange', 'alternate', 0, -1)])
        self.assertTrue(hasattr(middleware, 'mapper'))
        self.assertIsInstance(middleware.mapper, FakeMapper)
        self.assertEqual(middleware.mapper.kwargs, dict(register=False))
        self.assertEqual(len(middleware.mapper.routes), 2)
        for idx, route in enumerate(middleware.mapper.routes):
            self.assertIsInstance(route, FakeLimit)
            self.assertEqual(route.args, (db,))
            self.assertEqual(route.kwargs, dict(limit='limit%d' % (idx + 1)))
        self.assertEqual(daemon._pending.balance, 1)

    def test_reload_failure(self):
        self.stub_start()
        self.stub_reload(FakeFailingMapper)

        db = FakeDatabase()
        db._fakedb['errors'] = set()
        middleware = tests.GenericFakeClass()
        daemon = database.ControlDaemon(db, middleware, {})
        daemon._reload()

        self.assertEqual(len(self.log_messages), 1)
        self.assertTrue(self.log_messages[0].startswith(
                'Could not load limits'))
        self.assertEqual(len(db._actions), 3)
        self.assertEqual(db._actions[0], ('zrange', 'limits', 0, -1))
        self.assertEqual(db._actions[1][0], 'sadd')
        self.assertEqual(db._actions[1][1], 'errors')
        self.assertTrue(db._actions[1][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(db._actions[2][0], 'publish')
        self.assertEqual(db._actions[2][1], 'errors')
        self.assertTrue(db._actions[2][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(daemon._pending.balance, 1)

    def test_reload_failure_alternate(self):
        self.stub_start()
        self.stub_reload(FakeFailingMapper)

        db = FakeDatabase()
        db._fakedb['errors_set'] = set()
        middleware = tests.GenericFakeClass()
        daemon = database.ControlDaemon(db, middleware, dict(
                errors_key='errors_set',
                errors_channel='errors_channel',
                ))
        daemon._reload()

        self.assertEqual(len(self.log_messages), 1)
        self.assertTrue(self.log_messages[0].startswith(
                'Could not load limits'))
        self.assertEqual(len(db._actions), 3)
        self.assertEqual(db._actions[0], ('zrange', 'limits', 0, -1))
        self.assertEqual(db._actions[1][0], 'sadd')
        self.assertEqual(db._actions[1][1], 'errors_set')
        self.assertTrue(db._actions[1][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(db._actions[2][0], 'publish')
        self.assertEqual(db._actions[2][1], 'errors_channel')
        self.assertTrue(db._actions[2][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(daemon._pending.balance, 1)
