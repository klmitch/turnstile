# Copyright 2013 Rackspace
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import eventlet.semaphore
import mock
import unittest2

from turnstile import config
from turnstile import control


class TestException(Exception):
    pass


class TestLimitData(unittest2.TestCase):
    # MD5 sum of ''
    EMPTY_CHECKSUM = 'd41d8cd98f00b204e9800998ecf8427e'

    # Test data
    TEST_DATA = ['Nobody', 'inspects', 'the', 'spammish', 'repetition']

    # MD5 sum of the test data
    TEST_DATA_CHECKSUM = '2c79f652a24d3f4438b6b4034ae120cb'

    def test_init(self):
        ld = control.LimitData()

        self.assertEqual(ld.limit_data, [])
        self.assertEqual(ld.limit_sum, self.EMPTY_CHECKSUM)
        self.assertIsInstance(ld.limit_lock, eventlet.semaphore.Semaphore)

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_set_limits_nochange(self, mock_loads, mock_Semaphore):
        ld = control.LimitData()
        ld.limit_sum = self.TEST_DATA_CHECKSUM

        ld.set_limits(self.TEST_DATA)

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        self.assertFalse(mock_loads.called)
        self.assertEqual(ld.limit_data, [])
        self.assertEqual(ld.limit_sum, self.TEST_DATA_CHECKSUM)

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_set_limits(self, mock_loads, mock_Semaphore):
        ld = control.LimitData()

        ld.set_limits(self.TEST_DATA)

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        mock_loads.assert_has_calls([mock.call(x) for x in self.TEST_DATA])
        self.assertEqual(ld.limit_data, self.TEST_DATA)
        self.assertEqual(ld.limit_sum, self.TEST_DATA_CHECKSUM)

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits_nosum(self, mock_Semaphore):
        ld = control.LimitData()
        ld.limit_data = self.TEST_DATA
        ld.limit_sum = self.TEST_DATA_CHECKSUM

        result = ld.get_limits()

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        self.assertEqual(result, (self.TEST_DATA_CHECKSUM, self.TEST_DATA))

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits_nochange(self, mock_Semaphore):
        ld = control.LimitData()
        ld.limit_data = self.TEST_DATA
        ld.limit_sum = self.TEST_DATA_CHECKSUM

        self.assertRaises(control.NoChangeException, ld.get_limits,
                          self.TEST_DATA_CHECKSUM)
        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(control.NoChangeException, mock.ANY, mock.ANY),
        ])

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits(self, mock_Semaphore):
        ld = control.LimitData()
        ld.limit_data = self.TEST_DATA
        ld.limit_sum = self.TEST_DATA_CHECKSUM

        result = ld.get_limits(self.EMPTY_CHECKSUM)

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        self.assertEqual(result, (self.TEST_DATA_CHECKSUM, self.TEST_DATA))


class TestControlDaemon(unittest2.TestCase):
    @mock.patch.dict(control.ControlDaemon._commands)
    def test_register(self):
        self.assertEqual(control.ControlDaemon._commands, {
            'ping': control.ping,
            'reload': control.reload,
        })

        control.ControlDaemon._register('spam', 'ni')

        self.assertEqual(control.ControlDaemon._commands, {
            'ping': control.ping,
            'reload': control.reload,
            'spam': 'ni',
        })

    def test_init(self):
        cd = control.ControlDaemon('middleware', 'config')

        self.assertEqual(cd._db, None)
        self.assertEqual(cd.middleware, 'middleware')
        self.assertEqual(cd.config, 'config')
        self.assertIsInstance(cd.limits, control.LimitData)
        self.assertIsInstance(cd.pending, eventlet.semaphore.Semaphore)
        self.assertEqual(cd.listen_thread, None)

    @mock.patch.object(eventlet, 'spawn_n', return_value='listen_thread')
    @mock.patch.object(control.ControlDaemon, 'reload')
    def test_start(self, mock_reload, mock_spawn_n):
        cd = control.ControlDaemon('middleware', 'config')

        cd.start()

        mock_spawn_n.assert_called_once_with(cd.listen)
        self.assertEqual(cd.listen_thread, 'listen_thread')
        mock_reload.assert_called_once_with()

    @mock.patch.dict(control.ControlDaemon._commands, clear=True,
                     ping=mock.Mock(), _ping=mock.Mock(),
                     fail=mock.Mock(side_effect=TestException))
    @mock.patch.object(config.Config, 'get_database')
    @mock.patch.object(control.LOG, 'error')
    @mock.patch.object(control.LOG, 'exception')
    def test_listen(self, mock_exception, mock_error, mock_get_database):
        pubsub = mock.Mock(**{'listen.return_value': [
            {
                'type': 'other',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'other',
                'data': 'ping',
            },
            {
                'type': 'message',
                'channel': 'other',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': '',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': '',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': '_ping',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': '_ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'nosuch',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'nosuch',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'fail',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'fail',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'fail:arg1:arg2',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'fail:arg1:arg2',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'ping:arg1:arg2',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'ping:arg1:arg2',
            },
        ]})
        db = mock.Mock(**{'pubsub.return_value': pubsub})
        mock_get_database.return_value = db
        cd = control.ControlDaemon('middleware', config.Config())

        cd.listen()

        mock_get_database.assert_called_once_with('control')
        db.pubsub.assert_called_once_with()
        pubsub.assert_has_calls([
            mock.call.subscribe('control'),
            mock.call.listen(),
        ])
        mock_error.assert_has_calls([
            mock.call("Cannot call internal command '_ping'"),
            mock.call("Cannot call internal command '_ping'"),
            mock.call("No such command 'nosuch'"),
            mock.call("No such command 'nosuch'"),
        ])
        mock_exception.assert_has_calls([
            mock.call("Failed to execute command 'fail' arguments []"),
            mock.call("Failed to execute command 'fail' arguments []"),
            mock.call("Failed to execute command 'fail' arguments "
                      "['arg1', 'arg2']"),
            mock.call("Failed to execute command 'fail' arguments "
                      "['arg1', 'arg2']"),
        ])
        control.ControlDaemon._commands['ping'].assert_has_calls([
            mock.call(cd),
            mock.call(cd),
            mock.call(cd, 'arg1', 'arg2'),
            mock.call(cd, 'arg1', 'arg2'),
        ])
        self.assertFalse(control.ControlDaemon._commands['_ping'].called)
        control.ControlDaemon._commands['fail'].assert_has_calls([
            mock.call(cd),
            mock.call(cd),
            mock.call(cd, 'arg1', 'arg2'),
            mock.call(cd, 'arg1', 'arg2'),
        ])

    @mock.patch.dict(control.ControlDaemon._commands, clear=True,
                     ping=mock.Mock(), _ping=mock.Mock(),
                     fail=mock.Mock(side_effect=TestException))
    @mock.patch.object(config.Config, 'get_database')
    @mock.patch.object(control.LOG, 'error')
    @mock.patch.object(control.LOG, 'exception')
    def test_listen_altchan(self, mock_exception, mock_error,
                            mock_get_database):
        pubsub = mock.Mock(**{'listen.return_value': [
            {
                'type': 'other',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'other',
                'data': 'ping',
            },
            {
                'type': 'message',
                'channel': 'other',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': '',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': '',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': '_ping',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': '_ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'nosuch',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'nosuch',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'fail',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'fail',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'fail:arg1:arg2',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'fail:arg1:arg2',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'ping',
            },
            {
                'type': 'pmessage',
                'channel': 'control',
                'data': 'ping:arg1:arg2',
            },
            {
                'type': 'message',
                'channel': 'control',
                'data': 'ping:arg1:arg2',
            },
        ]})
        db = mock.Mock(**{'pubsub.return_value': pubsub})
        mock_get_database.return_value = db
        cd = control.ControlDaemon('middleware', config.Config(conf_dict={
            'control.channel': 'other',
        }))

        cd.listen()

        mock_get_database.assert_called_once_with('control')
        db.pubsub.assert_called_once_with()
        pubsub.assert_has_calls([
            mock.call.subscribe('other'),
            mock.call.listen(),
        ])
        self.assertFalse(mock_error.called)
        self.assertFalse(mock_exception.called)
        control.ControlDaemon._commands['ping'].assert_has_calls([
            mock.call(cd),
            mock.call(cd),
        ])
        self.assertFalse(control.ControlDaemon._commands['_ping'].called)
        self.assertFalse(control.ControlDaemon._commands['fail'].called)

    @mock.patch.object(config.Config, 'get_database')
    def test_listen_shardhint(self, mock_get_database):
        pubsub = mock.Mock(**{'listen.return_value': []})
        db = mock.Mock(**{'pubsub.return_value': pubsub})
        mock_get_database.return_value = db
        cd = control.ControlDaemon('middleware', config.Config(conf_dict={
            'control.shard_hint': 'shard',
        }))

        cd.listen()

        mock_get_database.assert_called_once_with('control')
        db.pubsub.assert_called_once_with(shard_hint='shard')
        pubsub.assert_has_calls([
            mock.call.subscribe('control'),
            mock.call.listen(),
        ])

    def test_get_limits(self):
        cd = control.ControlDaemon('middleware', 'config')
        cd.limits = 'limits'

        self.assertEqual(cd.get_limits(), 'limits')

    @mock.patch.object(control.LOG, 'exception')
    @mock.patch('traceback.format_exc', return_value='<traceback>')
    def test_reload_noacquire(self, mock_format_exc, mock_exception):
        cd = control.ControlDaemon('middleware', config.Config())
        cd.pending = mock.Mock(**{'acquire.return_value': False})
        cd.limits = mock.Mock()
        cd._db = mock.Mock()

        cd.reload()

        cd.pending.assert_has_calls([
            mock.call.acquire(False),
        ])
        self.assertEqual(len(cd.pending.method_calls), 1)
        self.assertEqual(len(cd.limits.method_calls), 0)
        self.assertEqual(len(cd._db.method_calls), 0)
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)

    @mock.patch.object(control.LOG, 'exception')
    @mock.patch('traceback.format_exc', return_value='<traceback>')
    def test_reload(self, mock_format_exc, mock_exception):
        cd = control.ControlDaemon('middleware', config.Config())
        cd.pending = mock.Mock(**{'acquire.return_value': True})
        cd.limits = mock.Mock()
        cd._db = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})

        cd.reload()

        cd.pending.assert_has_calls([
            mock.call.acquire(False),
            mock.call.release(),
        ])
        self.assertEqual(len(cd.pending.method_calls), 2)
        cd.limits.set_limits.assert_called_once_with(['limit1', 'limit2'])
        cd._db.assert_has_calls([
            mock.call.zrange('limits', 0, -1),
        ])
        self.assertEqual(len(cd._db.method_calls), 1)
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)

    @mock.patch.object(control.LOG, 'exception')
    @mock.patch('traceback.format_exc', return_value='<traceback>')
    def test_reload_altlimits(self, mock_format_exc, mock_exception):
        cd = control.ControlDaemon('middleware', config.Config(conf_dict={
            'control.limits_key': 'other',
        }))
        cd.pending = mock.Mock(**{'acquire.return_value': True})
        cd.limits = mock.Mock()
        cd._db = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})

        cd.reload()

        cd.pending.assert_has_calls([
            mock.call.acquire(False),
            mock.call.release(),
        ])
        self.assertEqual(len(cd.pending.method_calls), 2)
        cd.limits.set_limits.assert_called_once_with(['limit1', 'limit2'])
        cd._db.assert_has_calls([
            mock.call.zrange('other', 0, -1),
        ])
        self.assertEqual(len(cd._db.method_calls), 1)
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)

    @mock.patch.object(control.LOG, 'exception')
    @mock.patch('traceback.format_exc', return_value='<traceback>')
    def test_reload_exception(self, mock_format_exc, mock_exception):
        cd = control.ControlDaemon('middleware', config.Config())
        cd.pending = mock.Mock(**{'acquire.return_value': True})
        cd.limits = mock.Mock(**{'set_limits.side_effect': TestException})
        cd._db = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})

        cd.reload()

        cd.pending.assert_has_calls([
            mock.call.acquire(False),
            mock.call.release(),
        ])
        self.assertEqual(len(cd.pending.method_calls), 2)
        cd.limits.set_limits.assert_called_once_with(['limit1', 'limit2'])
        cd._db.assert_has_calls([
            mock.call.zrange('limits', 0, -1),
            mock.call.sadd('errors', 'Failed to load limits: <traceback>'),
            mock.call.publish('errors', 'Failed to load limits: <traceback>'),
        ])
        self.assertEqual(len(cd._db.method_calls), 3)
        mock_exception.assert_called_once_with('Could not load limits')
        mock_format_exc.assert_called_once_with()

    @mock.patch.object(control.LOG, 'exception')
    @mock.patch('traceback.format_exc', return_value='<traceback>')
    def test_reload_exception_altkeys(self, mock_format_exc, mock_exception):
        cd = control.ControlDaemon('middleware', config.Config(conf_dict={
            'control.errors_key': 'alt_err',
            'control.errors_channel': 'alt_chan',
        }))
        cd.pending = mock.Mock(**{'acquire.return_value': True})
        cd.limits = mock.Mock(**{'set_limits.side_effect': TestException})
        cd._db = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})

        cd.reload()

        cd.pending.assert_has_calls([
            mock.call.acquire(False),
            mock.call.release(),
        ])
        self.assertEqual(len(cd.pending.method_calls), 2)
        cd.limits.set_limits.assert_called_once_with(['limit1', 'limit2'])
        cd._db.assert_has_calls([
            mock.call.zrange('limits', 0, -1),
            mock.call.sadd('alt_err', 'Failed to load limits: <traceback>'),
            mock.call.publish('alt_chan',
                              'Failed to load limits: <traceback>'),
        ])
        self.assertEqual(len(cd._db.method_calls), 3)
        mock_exception.assert_called_once_with('Could not load limits')
        mock_format_exc.assert_called_once_with()

    def test_db_present(self):
        middleware = mock.Mock(db='midware_db')
        cd = control.ControlDaemon(middleware, config.Config())
        cd._db = 'cached_db'

        self.assertEqual(cd.db, 'cached_db')

    def test_db_middleware(self):
        middleware = mock.Mock(db='midware_db')
        cd = control.ControlDaemon(middleware, config.Config())

        self.assertEqual(cd.db, 'midware_db')


class TestRegister(unittest2.TestCase):
    @mock.patch.object(control.ControlDaemon, '_register')
    def test_as_function(self, mock_register):
        control.register('spam', 'func')

        mock_register.assert_called_once_with('spam', 'func')

    @mock.patch.object(control.ControlDaemon, '_register')
    def test_as_decorator(self, mock_register):
        @control.register('spam')
        def func():
            pass

        mock_register.assert_called_once_with('spam', func)


class TestPing(unittest2.TestCase):
    def test_ping_no_channel(self):
        conf = config.Config()
        db = mock.Mock()
        daemon = mock.Mock(config=conf, db=db)

        control.ping(daemon, '')

        self.assertFalse(db.publish.called)

    def test_ping_no_data_no_nodename(self):
        conf = config.Config()
        db = mock.Mock()
        daemon = mock.Mock(config=conf, db=db)

        control.ping(daemon, 'reply')

        db.publish.assert_called_once_with('reply', 'pong')

    def test_ping_with_data_no_nodename(self):
        conf = config.Config()
        db = mock.Mock()
        daemon = mock.Mock(config=conf, db=db)

        control.ping(daemon, 'reply', 'data')

        db.publish.assert_called_once_with('reply', 'pong::data')

    def test_ping_no_data_with_nodename(self):
        conf = config.Config(conf_dict={
            'control.node_name': 'node',
        })
        db = mock.Mock()
        daemon = mock.Mock(config=conf, db=db)

        control.ping(daemon, 'reply')

        db.publish.assert_called_once_with('reply', 'pong:node')

    def test_ping_with_data_with_nodename(self):
        conf = config.Config(conf_dict={
            'control.node_name': 'node',
        })
        db = mock.Mock()
        daemon = mock.Mock(config=conf, db=db)

        control.ping(daemon, 'reply', 'data')

        db.publish.assert_called_once_with('reply', 'pong:node:data')


class TestReload(unittest2.TestCase):
    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_basic(self, mock_random, mock_spawn_n, mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config())

        control.reload(daemon)

        self.assertFalse(mock_random.called)
        self.assertFalse(mock_spawn_after.called)
        mock_spawn_n.assert_called_once_with('reload')

    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_configured_spread(self, mock_random, mock_spawn_n,
                               mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config(conf_dict={
            'control.reload_spread': '20.4',
        }))

        control.reload(daemon)

        mock_random.assert_called_once_with()
        mock_spawn_after.assert_called_once_with(10.2, 'reload')
        self.assertFalse(mock_spawn_n.called)

    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_configured_spread_bad(self, mock_random, mock_spawn_n,
                                   mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config(conf_dict={
            'control.reload_spread': '20.4.3',
        }))

        control.reload(daemon)

        self.assertFalse(mock_random.called)
        self.assertFalse(mock_spawn_after.called)
        mock_spawn_n.assert_called_once_with('reload')

    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_configured_spread_override(self, mock_random, mock_spawn_n,
                                        mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config(conf_dict={
            'control.reload_spread': '20.4',
        }))

        control.reload(daemon, 'immediate')

        self.assertFalse(mock_random.called)
        self.assertFalse(mock_spawn_after.called)
        mock_spawn_n.assert_called_once_with('reload')

    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_forced_spread(self, mock_random, mock_spawn_n, mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config())

        control.reload(daemon, 'spread', '20.4')

        mock_random.assert_called_once_with()
        mock_spawn_after.assert_called_once_with(10.2, 'reload')
        self.assertFalse(mock_spawn_n.called)

    @mock.patch.object(eventlet, 'spawn_after')
    @mock.patch.object(eventlet, 'spawn_n')
    @mock.patch('random.random', return_value=0.5)
    def test_bad_spread_fallback(self, mock_random, mock_spawn_n,
                                 mock_spawn_after):
        daemon = mock.Mock(reload='reload', config=config.Config(conf_dict={
            'control.reload_spread': '40.8',
        }))

        control.reload(daemon, 'spread', '20.4.3')

        mock_random.assert_called_once_with()
        mock_spawn_after.assert_called_once_with(20.4, 'reload')
        self.assertFalse(mock_spawn_n.called)
