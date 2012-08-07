import random

import eventlet
import msgpack
import routes

from turnstile import control
from turnstile import limits

import tests
from tests import db_fixture


class ControlDaemonTest(control.ControlDaemon):
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


class FakeMapper(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeMapper, self).__init__(*args, **kwargs)

        self.routes = []


class FakeFailingMapper(FakeMapper):
    def __init__(self, *args, **kwargs):
        raise Exception("Fake-out")


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
        self.stubs.Set(control.ControlDaemon, '_start', lambda x: None)

    def stub_reload(self, mapper=FakeMapper):
        self.stubs.Set(msgpack, 'loads', lambda x: x)
        self.stubs.Set(limits, 'Limit', db_fixture.FakeLimit)
        self.stubs.Set(routes, 'Mapper', mapper)

    def test_init(self):
        self.stub_spawn(True)

        def fake_reload(obj):
            obj._reloaded = True

        self.stubs.Set(control.ControlDaemon, '_listen', lambda obj: 'listen')
        self.stubs.Set(control.ControlDaemon, '_reload', fake_reload)

        daemon = control.ControlDaemon('db', 'middleware', 'config')

        self.assertEqual(daemon._db, 'db')
        self.assertEqual(daemon._middleware, 'middleware')
        self.assertEqual(daemon._config, 'config')
        self.assertIsInstance(daemon._pending, eventlet.semaphore.Semaphore)
        self.assertEqual(daemon._listen_thread, 'listen')
        self.assertEqual(daemon._reloaded, True)

    def test_listen_basic(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (), {})])
        self.assertIsInstance(db._pubsub, db_fixture.PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, {})
        self.assertEqual(pubsub._subscriptions, set(['control']))

    def test_listen_shard(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware',
                                       dict(shard_hint='shard'))
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (),
                                        dict(shard_hint='shard'))])
        self.assertIsInstance(db._pubsub, db_fixture.PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, dict(shard_hint='shard'))
        self.assertEqual(pubsub._subscriptions, set(['control']))

    def test_listen_control(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware',
                                       dict(channel='spam'))
        daemon._listen()

        self.assertEqual(db._actions, [('pubsub', (), {})])
        self.assertIsInstance(db._pubsub, db_fixture.PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, {})
        self.assertEqual(pubsub._subscriptions, set(['spam']))

    def test_listen_nonmessage(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='nosuch',
                pattern=None,
                channel='control',
                data='test:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_wrongchannel(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='wrongchannel',
                data='test:foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_empty(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data=':foo'))
        daemon = ControlDaemonTest(db, 'middleware', {})
        daemon._listen()

        self.assertEqual(daemon._commands, [])

    def test_listen_internal(self):
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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
        db = db_fixture.FakeDatabase()
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

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware', {})
        daemon.ping(None)

        self.assertEqual(db._published, [])

    def test_ping_basic(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware', {})
        daemon.ping('pong')

        self.assertEqual(db._published, [('pong', 'pong')])

    def test_ping_basic_node(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware',
                                        dict(node_name='node'))
        daemon.ping('pong')

        self.assertEqual(db._published, [('pong', 'pong:node')])

    def test_ping_data(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware', {})
        daemon.ping('pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong::data')])

    def test_ping_data_node(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        daemon = control.ControlDaemon(db, 'middleware',
                                        dict(node_name='node'))
        daemon.ping('pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong:node:data')])

    def test_reload_command_noargs(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware', {})
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_noargs_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_noargs_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload()

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_badtype(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware', {})
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_badtype_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_badtype_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('badtype')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_immediate(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware', {})
        daemon.reload('immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_immediate_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware', {})
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23.5.3'))
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon._reload, (), {})
                ])

    def test_reload_command_spread_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_given(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware', {})
        daemon.reload('spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_bad_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread', '18.0.5')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon._reload, (), {})
                ])

    def test_reload_command_spread_given_configured(self):
        self.stub_start()
        self.stub_spawn()

        daemon = control.ControlDaemon('db', 'middleware',
                                        dict(reload_spread='23'))
        daemon.reload('spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon._reload, (), {})
                ])

    def test_reload_noacquire(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = control.ControlDaemon(db, middleware, {})
        daemon._pending.acquire()
        daemon._reload()

        self.assertEqual(db._actions, [])
        self.assertFalse(hasattr(middleware, 'mapper'))

    def test_reload(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = control.ControlDaemon(db, middleware, {})
        daemon._reload()

        self.assertEqual(db._actions, [('zrange', 'limits', 0, -1)])
        self.assertTrue(hasattr(middleware, 'mapper'))
        self.assertIsInstance(middleware.mapper, FakeMapper)
        self.assertEqual(middleware.mapper.kwargs, dict(register=False))
        self.assertEqual(len(middleware.mapper.routes), 2)
        for idx, route in enumerate(middleware.mapper.routes):
            self.assertIsInstance(route, db_fixture.FakeLimit)
            self.assertEqual(route.args, (db,))
            self.assertEqual(route.kwargs, dict(limit='limit%d' % (idx + 1)))
        self.assertEqual(daemon._pending.balance, 1)

    def test_reload_alternate(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['alternate'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        middleware = tests.GenericFakeClass()
        daemon = control.ControlDaemon(db, middleware,
                                        dict(limits_key='alternate'))
        daemon._reload()

        self.assertEqual(db._actions, [('zrange', 'alternate', 0, -1)])
        self.assertTrue(hasattr(middleware, 'mapper'))
        self.assertIsInstance(middleware.mapper, FakeMapper)
        self.assertEqual(middleware.mapper.kwargs, dict(register=False))
        self.assertEqual(len(middleware.mapper.routes), 2)
        for idx, route in enumerate(middleware.mapper.routes):
            self.assertIsInstance(route, db_fixture.FakeLimit)
            self.assertEqual(route.args, (db,))
            self.assertEqual(route.kwargs, dict(limit='limit%d' % (idx + 1)))
        self.assertEqual(daemon._pending.balance, 1)

    def test_reload_failure(self):
        self.stub_start()
        self.stub_reload(FakeFailingMapper)

        db = db_fixture.FakeDatabase()
        db._fakedb['errors'] = set()
        middleware = tests.GenericFakeClass()
        daemon = control.ControlDaemon(db, middleware, {})
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

        db = db_fixture.FakeDatabase()
        db._fakedb['errors_set'] = set()
        middleware = tests.GenericFakeClass()
        daemon = control.ControlDaemon(db, middleware, dict(
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
