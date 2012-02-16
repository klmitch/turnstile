import eventlet
import msgpack
import redis

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

    def execute_command(self, *args, **kwargs):
        self._actions.append(('execute_command', args[0], args[1:], kwargs))
        raise Exception("Unhandled command %s" % args[0])


class FakeControlDaemon(object):
    def __init__(self, db, middleware, config):
        self.db = db
        self.middleware = middleware
        self.config = config


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


class TestInitialize(tests.TestCase):
    imports = {
        'FakeParser': FakeParser,
        'FakeConnection': FakeConnection,
        'FakeConnectionPool': FakeConnectionPool,
        }

    def setUp(self):
        super(TestInitialize, self).setUp()

        self.stubs.Set(database, 'TurnstileRedis', FakeDatabase)
        self.stubs.Set(database, 'ControlDaemon', FakeControlDaemon)

    def test_missing_connection(self):
        with self.assertRaises(redis.ConnectionError):
            db, daemon = database.initialize('middleware', {})

    def test_host_connection(self):
        config = dict(
            host='example.com',
            port='1234',
            db='5',
            password='password',
            socket_timeout='321',
            )
        db, daemon = database.initialize('middleware', config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(
                host='example.com', port=1234, db=5, password='password',
                socket_timeout=321))
        self.assertEqual(daemon.db, db)
        self.assertEqual(daemon.middleware, 'middleware')
        self.assertEqual(daemon.config, config)

    def test_host_minimal(self):
        config = dict(host='example.com')
        db, daemon = database.initialize('middleware', config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(host='example.com'))
        self.assertEqual(daemon.db, db)
        self.assertEqual(daemon.middleware, 'middleware')
        self.assertEqual(daemon.config, config)

    def test_unix_connection(self):
        config = dict(
            unix_socket_path='/tmp/redis',
            db='5',
            password='password',
            socket_timeout='321',
            )
        db, daemon = database.initialize('middleware', config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(
                unix_socket_path='/tmp/redis', db=5, password='password',
                socket_timeout=321))
        self.assertEqual(daemon.db, db)
        self.assertEqual(daemon.middleware, 'middleware')
        self.assertEqual(daemon.config, config)

    def test_unix_minimal(self):
        config = dict(unix_socket_path='/tmp/redis')
        db, daemon = database.initialize('middleware', config)

        self.assertEqual(db._args, ())
        self.assertEqual(db._kwargs, dict(unix_socket_path='/tmp/redis'))
        self.assertEqual(daemon.db, db)
        self.assertEqual(daemon.middleware, 'middleware')
        self.assertEqual(daemon.config, config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
        db, daemon = database.initialize('middleware', config)

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
    def test_init(self):
        def fake_reload(obj):
            obj._reloaded = True

        def fake_spawn(method, *args, **kwargs):
            return method(*args, **kwargs)

        self.stubs.Set(database.ControlDaemon, '_listen', lambda obj: 'listen')
        self.stubs.Set(database.ControlDaemon, '_reload', fake_reload)
        self.stubs.Set(eventlet, 'spawn_n', fake_spawn)

        daemon = database.ControlDaemon('db', 'middleware', 'config')

        self.assertEqual(daemon._db, 'db')
        self.assertEqual(daemon._middleware, 'middleware')
        self.assertEqual(daemon._config, 'config')
        self.assertIsInstance(daemon._pending, eventlet.semaphore.Semaphore)
        self.assertEqual(daemon._listen_thread, 'listen')
        self.assertEqual(daemon._reloaded, True)

    def test_listen_basic(self):
        self.stubs.Set(database.ControlDaemon, '_start', lambda x: None)

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
        self.stubs.Set(database.ControlDaemon, '_start', lambda x: None)

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
        self.stubs.Set(database.ControlDaemon, '_start', lambda x: None)

        db = FakeDatabase()
        daemon = database.ControlDaemon(db, 'middleware',
                                        dict(control_channel='spam'))
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
        messages = self.log_messages
        self.assertEqual(len(messages), 1)
        self.assertTrue(messages[0].startswith(
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
        messages = self.log_messages
        self.assertEqual(len(messages), 1)
        self.assertTrue(messages[0].startswith(
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
                                   dict(control_channel='alternate'))
        daemon._listen()

        self.assertEqual(daemon._commands, [('test', 'arg')])
        self.assertEqual(self.log_messages, [])
