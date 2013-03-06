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

import mock
import redis
import unittest2

from turnstile import database
from turnstile import limits
from turnstile import utils


class TestInitialize(unittest2.TestCase):
    def make_entrypoints(self, mock_find_entrypoint, **entrypoints):
        def fake_find_entrypoint(group, name, compat=True, required=False):
            try:
                return entrypoints[name]
            except KeyError:
                raise ImportError(name)

        mock_find_entrypoint.side_effect = fake_find_entrypoint

        return entrypoints

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_empty_config(self, mock_find_entrypoint, mock_ConnectionPool,
                          mock_StrictRedis):
        self.assertRaises(redis.ConnectionError, database.initialize, {})

        self.assertFalse(mock_ConnectionPool.called)
        self.assertFalse(mock_StrictRedis.called)

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_host_only(self, mock_find_entrypoint, mock_ConnectionPool,
                       mock_StrictRedis):
        result = database.initialize(dict(host='10.0.0.1'))

        self.assertEqual(result, 'db_handle')
        self.assertFalse(mock_ConnectionPool.called)
        mock_StrictRedis.assert_called_once_with(host='10.0.0.1')

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_unixpath_only(self, mock_find_entrypoint, mock_ConnectionPool,
                           mock_StrictRedis):
        result = database.initialize(dict(unix_socket_path='/tmp/socket'))

        self.assertEqual(result, 'db_handle')
        self.assertFalse(mock_ConnectionPool.called)
        mock_StrictRedis.assert_called_once_with(
            unix_socket_path='/tmp/socket')

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_alt_client(self, mock_find_entrypoint, mock_ConnectionPool,
                        mock_StrictRedis):
        entrypoints = self.make_entrypoints(
            mock_find_entrypoint,
            client=mock.Mock(return_value='alt_handle'),
        )

        result = database.initialize(dict(host='10.0.0.1',
                                          redis_client='client'))

        self.assertEqual(result, 'alt_handle')
        self.assertFalse(mock_ConnectionPool.called)
        self.assertFalse(mock_StrictRedis.called)
        entrypoints['client'].assert_called_once_with(host='10.0.0.1')

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_all_options(self, mock_find_entrypoint, mock_ConnectionPool,
                         mock_StrictRedis):
        result = database.initialize({
            'host': '10.0.0.1',
            'port': '1234',
            'db': '5',
            'password': 'spampass',
            'socket_timeout': '600',
            'unix_socket_path': '/tmp/redis',
        })

        self.assertEqual(result, 'db_handle')
        self.assertFalse(mock_ConnectionPool.called)
        mock_StrictRedis.assert_called_once_with(
            host='10.0.0.1',
            port=1234,
            db=5,
            password='spampass',
            socket_timeout=600,
            unix_socket_path='/tmp/redis',
        )

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_cpool_options(self, mock_find_entrypoint, mock_ConnectionPool,
                           mock_StrictRedis):
        entrypoints = self.make_entrypoints(
            mock_find_entrypoint,
            connection='connection_fake',
            parser='parser_fake',
        )

        result = database.initialize({
            'host': '10.0.0.1',
            'port': '1234',
            'db': '5',
            'password': 'spampass',
            'socket_timeout': '600',
            'unix_socket_path': '/tmp/redis',
            'connection_pool.connection_class': 'connection',
            'connection_pool.max_connections': '50',
            'connection_pool.parser_class': 'parser',
            'connection_pool.other': 'value',
        })

        self.assertEqual(result, 'db_handle')
        mock_StrictRedis.assert_called_once_with(connection_pool='conn_pool')
        mock_ConnectionPool.assert_called_once_with(
            host='10.0.0.1',
            port=1234,
            db=5,
            password='spampass',
            socket_timeout=600,
            unix_socket_path='/tmp/redis',
            connection_class='connection_fake',
            max_connections=50,
            parser_class='parser_fake',
            other='value',
        )

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_cpool_options_altpool(self, mock_find_entrypoint,
                                   mock_ConnectionPool, mock_StrictRedis):
        entrypoints = self.make_entrypoints(
            mock_find_entrypoint,
            connection='connection_fake',
            parser='parser_fake',
            pool=mock.Mock(return_value='pool_fake'),
        )

        result = database.initialize({
            'host': '10.0.0.1',
            'port': '1234',
            'db': '5',
            'password': 'spampass',
            'socket_timeout': '600',
            'unix_socket_path': '/tmp/redis',
            'connection_pool': 'pool',
            'connection_pool.connection_class': 'connection',
            'connection_pool.max_connections': '50',
            'connection_pool.parser_class': 'parser',
            'connection_pool.other': 'value',
        })

        self.assertEqual(result, 'db_handle')
        mock_StrictRedis.assert_called_once_with(connection_pool='pool_fake')
        self.assertFalse(mock_ConnectionPool.called)
        entrypoints['pool'].assert_called_once_with(
            host='10.0.0.1',
            port=1234,
            db=5,
            password='spampass',
            socket_timeout=600,
            unix_socket_path='/tmp/redis',
            connection_class='connection_fake',
            max_connections=50,
            parser_class='parser_fake',
            other='value',
        )

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_cpool_unixsock(self, mock_find_entrypoint, mock_ConnectionPool,
                            mock_StrictRedis):
        result = database.initialize({
            'host': '10.0.0.1',
            'port': '1234',
            'unix_socket_path': '/tmp/redis',
            'connection_pool.other': 'value',
        })

        self.assertEqual(result, 'db_handle')
        mock_StrictRedis.assert_called_once_with(connection_pool='conn_pool')
        mock_ConnectionPool.assert_called_once_with(
            path='/tmp/redis',
            other='value',
            connection_class=redis.UnixDomainSocketConnection,
        )

    @mock.patch.object(redis, 'StrictRedis', return_value='db_handle')
    @mock.patch.object(redis, 'ConnectionPool', return_value='conn_pool')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_cpool_host(self, mock_find_entrypoint, mock_ConnectionPool,
                        mock_StrictRedis):
        result = database.initialize({
            'host': '10.0.0.1',
            'port': '1234',
            'connection_pool.other': 'value',
        })

        self.assertEqual(result, 'db_handle')
        mock_StrictRedis.assert_called_once_with(connection_pool='conn_pool')
        mock_ConnectionPool.assert_called_once_with(
            host='10.0.0.1',
            port=1234,
            other='value',
            connection_class=redis.Connection,
        )


class TestLimitsHydrate(unittest2.TestCase):
    @mock.patch.object(limits.Limit, 'hydrate',
                       side_effect=lambda x, y: "limit:%s" % y)
    def test_hydrate(self, mock_hydrate):
        result = database.limits_hydrate('db', ['lim1', 'lim2', 'lim3'])

        self.assertEqual(result, ['limit:lim1', 'limit:lim2', 'limit:lim3'])
        mock_hydrate.assert_has_calls([
            mock.call('db', 'lim1'),
            mock.call('db', 'lim2'),
            mock.call('db', 'lim3'),
        ])


class TestLimitUpdate(unittest2.TestCase):
    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    def test_limit_update(self, mock_dumps):
        limits = [
            mock.Mock(**{'dehydrate.return_value': 'limit1'}),
            mock.Mock(**{'dehydrate.return_value': 'limit2'}),
            mock.Mock(**{'dehydrate.return_value': 'limit3'}),
            mock.Mock(**{'dehydrate.return_value': 'limit4'}),
            mock.Mock(**{'dehydrate.return_value': 'limit5'}),
            mock.Mock(**{'dehydrate.return_value': 'limit6'}),
        ]
        pipe = mock.MagicMock(**{
            'zrange.return_value': [
                'limit2',
                'limit4',
                'limit6',
                'limit8',
            ],
        })
        pipe.__enter__.return_value = pipe
        pipe.__exit__.return_value = False
        db = mock.Mock(**{'pipeline.return_value': pipe})

        database.limit_update(db, 'limit_key', limits)
        for lim in limits:
            lim.dehydrate.assert_called_once_with()
        mock_dumps.assert_has_calls([
            mock.call('limit1'),
            mock.call('limit2'),
            mock.call('limit3'),
            mock.call('limit4'),
            mock.call('limit5'),
            mock.call('limit6'),
        ])
        db.pipeline.assert_called_once_with()
        pipe.assert_has_calls([
            mock.call.__enter__(),
            mock.call.watch('limit_key'),
            mock.call.zrange('limit_key', 0, -1),
            mock.call.multi(),
            mock.call.zrem('limit_key', 'limit8'),
            mock.call.zadd('limit_key', 10, 'limit1'),
            mock.call.zadd('limit_key', 20, 'limit2'),
            mock.call.zadd('limit_key', 30, 'limit3'),
            mock.call.zadd('limit_key', 40, 'limit4'),
            mock.call.zadd('limit_key', 50, 'limit5'),
            mock.call.zadd('limit_key', 60, 'limit6'),
            mock.call.execute(),
            mock.call.__exit__(None, None, None),
        ])

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    def test_limit_update_retry(self, mock_dumps):
        limits = [
            mock.Mock(**{'dehydrate.return_value': 'limit1'}),
            mock.Mock(**{'dehydrate.return_value': 'limit2'}),
            mock.Mock(**{'dehydrate.return_value': 'limit3'}),
            mock.Mock(**{'dehydrate.return_value': 'limit4'}),
            mock.Mock(**{'dehydrate.return_value': 'limit5'}),
            mock.Mock(**{'dehydrate.return_value': 'limit6'}),
        ]
        pipe = mock.MagicMock(**{
            'zrange.return_value': [
                'limit2',
                'limit4',
                'limit6',
                'limit8',
            ],
            'execute.side_effect': [redis.WatchError, None],
        })
        pipe.__enter__.return_value = pipe
        pipe.__exit__.return_value = False
        db = mock.Mock(**{'pipeline.return_value': pipe})

        database.limit_update(db, 'limit_key', limits)
        for lim in limits:
            lim.dehydrate.assert_called_once_with()
        mock_dumps.assert_has_calls([
            mock.call('limit1'),
            mock.call('limit2'),
            mock.call('limit3'),
            mock.call('limit4'),
            mock.call('limit5'),
            mock.call('limit6'),
        ])
        db.pipeline.assert_called_once_with()
        pipe.assert_has_calls([
            mock.call.__enter__(),
            mock.call.watch('limit_key'),
            mock.call.zrange('limit_key', 0, -1),
            mock.call.multi(),
            mock.call.zrem('limit_key', 'limit8'),
            mock.call.zadd('limit_key', 10, 'limit1'),
            mock.call.zadd('limit_key', 20, 'limit2'),
            mock.call.zadd('limit_key', 30, 'limit3'),
            mock.call.zadd('limit_key', 40, 'limit4'),
            mock.call.zadd('limit_key', 50, 'limit5'),
            mock.call.zadd('limit_key', 60, 'limit6'),
            mock.call.execute(),
            mock.call.watch('limit_key'),
            mock.call.zrange('limit_key', 0, -1),
            mock.call.multi(),
            mock.call.zrem('limit_key', 'limit8'),
            mock.call.zadd('limit_key', 10, 'limit1'),
            mock.call.zadd('limit_key', 20, 'limit2'),
            mock.call.zadd('limit_key', 30, 'limit3'),
            mock.call.zadd('limit_key', 40, 'limit4'),
            mock.call.zadd('limit_key', 50, 'limit5'),
            mock.call.zadd('limit_key', 60, 'limit6'),
            mock.call.execute(),
            mock.call.__exit__(None, None, None),
        ])


class TestCommand(unittest2.TestCase):
    def test_command(self):
        db = mock.Mock()

        database.command(db, 'channel', 'command', 'one', 2, 3.14)

        db.publish.assert_called_once_with('channel', 'command:one:2:3.14')
