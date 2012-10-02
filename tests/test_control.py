import hashlib
import random

import eventlet
import msgpack

from turnstile import control
from turnstile import limits

import tests
from tests import db_fixture


class ControlDaemonTest(control.ControlDaemon):
    def __init__(self, middleware, conf):
        super(ControlDaemonTest, self).__init__(middleware, conf)

        self._commands = self._commands.copy()
        self._commands['_internal'] = self._internal
        self._commands['test'] = self.test
        self._commands['failure'] = self.failure
        self._command_log = []

    def start(self):
        pass

    def _internal(self, daemon, *args):
        self._command_log.append(('internal', args))

    def test(self, daemon, arg):
        self._command_log.append(('test', arg))

    def failure(self, daemon, *args):
        self._command_log.append(('failure', args))
        raise Exception("Failure")


class TestLimitData(tests.TestCase):
    def setUp(self):
        super(TestLimitData, self).setUp()

        self.stubs.Set(limits, 'Limit', db_fixture.FakeLimit)
        self.stubs.Set(msgpack, 'loads', lambda x: dict(limit=x))

        # Generate some interesting data for use by the tests
        chksum = hashlib.md5()
        chksum.update('')
        self.empty_chksum = chksum.hexdigest()

        self.test_data = ["Nobody", "inspects", "the", "spammish",
                          "repetition"]
        chksum = hashlib.md5()
        for datum in self.test_data:
            chksum.update(datum)
        self.test_chksum = chksum.hexdigest()

    def test_init(self):
        ld = control.LimitData()

        # Test that this is initialized properly
        self.assertEqual(ld.limit_data, [])
        self.assertEqual(ld.limit_sum, self.empty_chksum)
        self.assertIsInstance(ld.limit_lock, eventlet.semaphore.Semaphore)

    def test_set_limits(self):
        ld = control.LimitData()

        # Set the test data...
        ld.set_limits(self.test_data)

        self.assertEqual(ld.limit_data,
                         [dict(limit=lim) for lim in self.test_data])
        self.assertEqual(ld.limit_sum, self.test_chksum)
        self.assertEqual(ld.limit_lock.balance, 1)

    def test_get_limits_nosum(self):
        ld = control.LimitData()
        ld.limit_data = self.test_data
        ld.limit_sum = self.test_chksum

        chksum, lims = ld.get_limits()

        self.assertEqual(chksum, self.test_chksum)
        self.assertEqual(len(lims), len(self.test_data))
        for idx, lim in enumerate(lims):
            self.assertEqual(lim, self.test_data[idx])

    def test_get_limits_wrongsum(self):
        ld = control.LimitData()
        ld.limit_data = self.test_data
        ld.limit_sum = self.test_chksum

        chksum, lims = ld.get_limits(self.empty_chksum)

        self.assertEqual(chksum, self.test_chksum)
        self.assertEqual(len(lims), len(self.test_data))
        for idx, lim in enumerate(lims):
            self.assertEqual(lim, self.test_data[idx])

    def test_get_limits_samesum(self):
        ld = control.LimitData()
        ld.limit_data = self.test_data
        ld.limit_sum = self.test_chksum

        self.assertRaises(control.NoChangeException, ld.get_limits,
                          self.test_chksum)


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
        self.stubs.Set(control.ControlDaemon, 'start', lambda x: None)

    def stub_reload(self):
        self.stubs.Set(control, 'LimitData', db_fixture.FakeLimitData)

    def test_init(self):
        self.stub_spawn(True)

        def fake_reload(obj):
            obj._reloaded = True

        self.stubs.Set(control.ControlDaemon, 'listen', lambda obj: 'listen')
        self.stubs.Set(control.ControlDaemon, 'reload', fake_reload)

        mid = tests.FakeMiddleware()
        daemon = control.ControlDaemon(mid, 'config')
        daemon._reloaded = False

        self.assertEqual(daemon.middleware, mid)
        self.assertEqual(daemon.config, 'config')
        self.assertIsInstance(daemon.pending, eventlet.semaphore.Semaphore)
        self.assertEqual(daemon.listen_thread, None)
        self.assertEqual(daemon._reloaded, False)

    def test_start(self):
        self.stub_spawn(True)

        def fake_reload(obj):
            obj._reloaded = True

        self.stubs.Set(control.ControlDaemon, 'listen', lambda obj: 'listen')
        self.stubs.Set(control.ControlDaemon, 'reload', fake_reload)

        daemon = control.ControlDaemon(tests.FakeMiddleware(), 'config')
        daemon._reloaded = False
        daemon.start()

        self.assertEqual(daemon.listen_thread, 'listen')
        self.assertEqual(daemon._reloaded, True)

    def test_listen_basic(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(db._actions, [('pubsub', (), {})])
        self.assertIsInstance(db._pubsub, db_fixture.PubSub)

        pubsub = db._pubsub
        self.assertEqual(pubsub._args, ())
        self.assertEqual(pubsub._kwargs, {})
        self.assertEqual(pubsub._subscriptions, set(['control']))

    def test_listen_shard(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig(dict(shard_hint='shard'), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.listen()

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
        conf = tests.FakeConfig(dict(channel='spam'), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.listen()

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
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])

    def test_listen_wrongchannel(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='wrongchannel',
                data='test:foo'))
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])

    def test_listen_empty(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data=':foo'))
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])

    def test_listen_internal(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='_internal:foo'))
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])
        self.assertEqual(self.log_messages, [
                "Cannot call internal command '_internal'",
                ])

    def test_listen_unknown(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='unknown:foo'))
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])
        self.assertEqual(self.log_messages, [
                "No such command 'unknown'",
                ])

    def test_listen_badargs(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='control',
                data='test:arg1:arg2'))
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [])
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
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [('failure', ('arg1', 'arg2'))])
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
        conf = tests.FakeConfig({}, db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [('test', 'arg')])
        self.assertEqual(self.log_messages, [])

    def test_listen_callout_alternate_channel(self):
        db = db_fixture.FakeDatabase()
        db._messages.append(dict(
                type='message',
                pattern=None,
                channel='alternate',
                data='test:arg'))
        conf = tests.FakeConfig(dict(channel='alternate'), db)
        daemon = ControlDaemonTest(tests.FakeMiddleware(conf), conf)
        daemon.listen()

        self.assertEqual(daemon._command_log, [('test', 'arg')])
        self.assertEqual(self.log_messages, [])

    def test_ping_nochan(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.ping(daemon, None)

        self.assertEqual(db._published, [])

    def test_ping_basic(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.ping(daemon, 'pong')

        self.assertEqual(db._published, [('pong', 'pong')])

    def test_ping_basic_node(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig(dict(node_name='node'), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.ping(daemon, 'pong')

        self.assertEqual(db._published, [('pong', 'pong:node')])

    def test_ping_data(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.ping(daemon, 'pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong::data')])

    def test_ping_data_node(self):
        self.stub_start()

        db = db_fixture.FakeDatabase()
        conf = tests.FakeConfig(dict(node_name='node'), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.ping(daemon, 'pong', 'data')

        self.assertEqual(db._published, [('pong', 'pong:node:data')])

    def test_reload_command_noargs(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig({})
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon)

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_noargs_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23.5.3'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon)

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_noargs_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon)

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon.reload, (), {})
                ])

    def test_reload_command_badtype(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig({})
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_badtype_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23.5.3'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'badtype')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_badtype_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'badtype')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon.reload, (), {})
                ])

    def test_reload_command_immediate(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig({})
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_immediate_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'immediate')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_spread(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig({})
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_spread_configured_bad(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23.5.3'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread')

        self.assertEqual(self.spawns, [
                ('spawn_n', daemon.reload, (), {})
                ])

    def test_reload_command_spread_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon.reload, (), {})
                ])

    def test_reload_command_spread_given(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig({})
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon.reload, (), {})
                ])

    def test_reload_command_spread_bad_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread', '18.0.5')

        self.assertEqual(self.spawns, [
                ('spawn_after', 23.0, daemon.reload, (), {})
                ])

    def test_reload_command_spread_given_configured(self):
        self.stub_start()
        self.stub_spawn()

        conf = tests.FakeConfig(dict(reload_spread='23'))
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        control.reload(daemon, 'spread', '18')

        self.assertEqual(self.spawns, [
                ('spawn_after', 18.0, daemon.reload, (), {})
                ])

    def test_reload_noacquire(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        conf = tests.FakeConfig({}, db)
        middleware = tests.FakeMiddleware(conf)
        daemon = control.ControlDaemon(middleware, conf)
        daemon.pending.acquire()
        daemon.reload()

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
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.reload()

        self.assertEqual(db._actions, [('zrange', 'limits', 0, -1)])
        self.assertEqual(daemon.limits.limit_data, [dict(limit='limit1'),
                                                    dict(limit='limit2')])
        self.assertEqual(daemon.limits.limit_sum, 2)
        self.assertEqual(daemon.pending.balance, 1)

    def test_reload_alternate(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['alternate'] = [
            (10, dict(limit='limit1')),
            (20, dict(limit='limit2')),
            ]
        conf = tests.FakeConfig(dict(limits_key='alternate'), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.reload()

        self.assertEqual(db._actions, [('zrange', 'alternate', 0, -1)])
        self.assertEqual(daemon.limits.limit_data, [dict(limit='limit1'),
                                                    dict(limit='limit2')])
        self.assertEqual(daemon.limits.limit_sum, 2)
        self.assertEqual(daemon.pending.balance, 1)

    def test_reload_failure(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = []
        db._fakedb['errors'] = set()
        conf = tests.FakeConfig({}, db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.reload()

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
        self.assertEqual(daemon.pending.balance, 1)

    def test_reload_failure_alternate(self):
        self.stub_start()
        self.stub_reload()

        db = db_fixture.FakeDatabase()
        db._fakedb['limits'] = []
        db._fakedb['errors_set'] = set()
        conf = tests.FakeConfig(dict(
                errors_key='errors_set',
                errors_channel='errors_channel',
                ), db)
        daemon = control.ControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.reload()

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
        self.assertEqual(daemon.pending.balance, 1)
