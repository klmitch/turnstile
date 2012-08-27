import json

import msgpack
import redis

from turnstile import database
from turnstile import limits

import tests
from tests import db_fixture


class FakeParser(tests.GenericFakeClass):
    pass


class FakeConnection(object):
    pass


class FakeConnectionPool(tests.GenericFakeClass):
    pass


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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
        limits = [
            db_fixture.FakeLimit(name='limit1'),
            db_fixture.FakeLimit(name='limit2'),
            db_fixture.FakeLimit(name='limit3'),
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = [
            (10, '{"name": "limit1"}'),
            (20, '{"name": "limit3"}'),
            (30, '{"name": "limit4"}'),
            ]
        limits = [
            db_fixture.FakeLimit(name='limit1'),
            db_fixture.FakeLimit(name='limit2'),
            db_fixture.FakeLimit(name='limit3'),
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
        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = [
            (10, '{"name": "limit1"}'),
            (20, '{"name": "limit3"}'),
            (30, '{"name": "limit4"}'),
            ]
        db._watcherror['limits'] = 1
        limits = [
            db_fixture.FakeLimit(name='limit1'),
            db_fixture.FakeLimit(name='limit2'),
            db_fixture.FakeLimit(name='limit3'),
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
        db = db_fixture.FakeDatabase()
        db.command('control', 'foo')

        self.assertEqual(db._published, [('control', 'foo')])

    def test_command_args(self):
        db = db_fixture.FakeDatabase()
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

        self.stubs.Set(database, 'TurnstileRedis', db_fixture.FakeDatabase)

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


class TestLimitsHydrate(tests.TestCase):
    def setUp(self):
        super(TestLimitsHydrate, self).setUp()

        self.stubs.Set(msgpack, 'loads', lambda x: x)
        self.stubs.Set(limits, 'Limit', db_fixture.FakeLimit)

    def test_limits_hydrate(self):
        exemplar = [dict(limit=i)
                    for i in ["Nobody", "inspects", "the",
                              "spammish", "repetition"]]
        print exemplar
        result = database.limits_hydrate('db', exemplar)

        self.assertEqual(len(result), len(exemplar))
        for idx, lim in enumerate(result):
            self.assertEqual(lim.args, ('db',))
            self.assertEqual(lim.kwargs, exemplar[idx])
