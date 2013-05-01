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
import unittest2

from turnstile import config
from turnstile import database


class TestConfig(unittest2.TestCase):
    @mock.patch('ConfigParser.SafeConfigParser')
    def test_init_empty(self, mock_SafeConfigParser):
        cfg = config.Config()

        self.assertEqual(cfg._config, {
            None: {
                'status': '413 Request Entity Too Large',
            },
        })
        self.assertFalse(mock_SafeConfigParser.called)

    @mock.patch('ConfigParser.SafeConfigParser')
    def test_init_dict(self, mock_SafeConfigParser):
        local_conf = {
            'preprocess': 'foo:bar',
            'redis.host': '10.0.0.1',
            'control.channel': 'control_channel',
            'control.connection_pool.connection': 'FoobarConnection',
        }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg._config, {
            None: {
                'status': '413 Request Entity Too Large',
                'preprocess': 'foo:bar',
            },
            'redis': {
                'host': '10.0.0.1',
            },
            'control': {
                'channel': 'control_channel',
                'connection_pool.connection': 'FoobarConnection',
            },
        })
        self.assertFalse(mock_SafeConfigParser.called)

    @mock.patch('ConfigParser.SafeConfigParser', return_value=mock.Mock(**{
        'sections.return_value': [],
    }))
    def test_init_files(self, mock_SafeConfigParser):
        local_conf = {
            'config': 'file_from_dict',
        }

        cfg = config.Config(conf_dict=local_conf, conf_file='file_from_args')

        self.assertEqual(cfg._config, {
            None: {
                'status': '413 Request Entity Too Large',
                'config': 'file_from_dict',
            },
        })
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.read.assert_called_once_with(
            ['file_from_dict', 'file_from_args'])

    @mock.patch('ConfigParser.SafeConfigParser', return_value=mock.Mock())
    def test_init_from_files(self, mock_SafeConfigParser):
        items = {
            'turnstile': [
                ('preprocess', 'foo:bar'),
            ],
            'redis': [
                ('password', 'spampass'),
            ],
            'control': [
                ('channel', 'control_channel'),
                ('connection_pool', 'FoobarConnectionPool'),
            ],
        }
        mock_SafeConfigParser.return_value.sections.return_value = \
            ['turnstile', 'redis', 'control']
        mock_SafeConfigParser.return_value.items.side_effect = \
            lambda x: items[x]
        local_conf = {
            'config': 'file_from_dict',
            'status': '500 Internal Error',
            'redis.host': '10.0.0.1',
        }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg._config, {
            None: {
                'status': '500 Internal Error',
                'config': 'file_from_dict',
                'preprocess': 'foo:bar',
            },
            'redis': {
                'host': '10.0.0.1',
                'password': 'spampass',
            },
            'control': {
                'channel': 'control_channel',
                'connection_pool': 'FoobarConnectionPool',
            },
        })
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.assert_has_calls([
            mock.call.read(['file_from_dict']),
            mock.call.sections(),
            mock.call.items('turnstile'),
            mock.call.items('redis'),
            mock.call.items('control'),
        ])

    @mock.patch('ConfigParser.SafeConfigParser')
    def test_getitem(self, mock_SafeConfigParser):
        local_conf = {
            'preprocess': 'foo:bar',
            'redis.host': '10.0.0.1',
            'control.channel': 'control_channel',
            'control.connection_pool.connection': 'FoobarConnection',
        }
        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg['redis'], dict(host='10.0.0.1'))
        self.assertEqual(cfg['nosuch'], {})

    @mock.patch('ConfigParser.SafeConfigParser')
    def test_contains(self, mock_SafeConfigParser):
        local_conf = {
            'preprocess': 'foo:bar',
            'redis.host': '10.0.0.1',
            'control.channel': 'control_channel',
            'control.connection_pool.connection': 'FoobarConnection',
        }
        cfg = config.Config(conf_dict=local_conf)

        self.assertTrue('redis' in cfg)
        self.assertFalse('nosuch' in cfg)

    @mock.patch('ConfigParser.SafeConfigParser')
    def test_getattr(self, mock_SafeConfigParser):
        local_conf = {
            'preprocess': 'foo:bar',
            'redis.host': '10.0.0.1',
            'control.channel': 'control_channel',
            'control.connection_pool.connection': 'FoobarConnection',
        }
        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg.preprocess, 'foo:bar')
        with self.assertRaises(AttributeError):
            dummy = cfg.nosuch

    @mock.patch('ConfigParser.SafeConfigParser')
    def test_get(self, mock_SafeConfigParser):
        local_conf = {
            'preprocess': 'foo:bar',
            'redis.host': '10.0.0.1',
            'control.channel': 'control_channel',
            'control.connection_pool.connection': 'FoobarConnection',
        }
        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg.get('preprocess'), 'foo:bar')
        self.assertEqual(cfg.get('nosuch'), None)
        self.assertEqual(cfg.get('nosuch', 'other'), 'other')

    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(database, 'initialize', return_value='db_handle')
    def test_get_database_basic(self, mock_initialize, mock_SafeConfigParser):
        local_conf = {
            'redis.host': '10.0.0.1',
            'redis.password': 'spampass',
            'redis.db': '3',
            'control.host': '10.0.0.2',
            'control.redis.host': '10.0.0.11',
            'control.redis.password': 'passspam',
            'control.redis.port': '1234',
        }
        cfg = config.Config(conf_dict=local_conf)

        result = cfg.get_database()

        self.assertEqual(result, 'db_handle')
        mock_initialize.assert_called_once_with({
            'host': '10.0.0.1',
            'password': 'spampass',
            'db': '3',
        })
        self.assertEqual(cfg._config, {
            None: {
                'status': '413 Request Entity Too Large',
            },
            'redis': {
                'host': '10.0.0.1',
                'password': 'spampass',
                'db': '3',
            },
            'control': {
                'host': '10.0.0.2',
                'redis.host': '10.0.0.11',
                'redis.password': 'passspam',
                'redis.port': '1234',
            },
        })

    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(database, 'initialize', return_value='db_handle')
    def test_get_database_override(self, mock_initialize,
                                   mock_SafeConfigParser):
        local_conf = {
            'redis.host': '10.0.0.1',
            'redis.password': 'spampass',
            'redis.db': '3',
            'control.host': '10.0.0.2',
            'control.redis.host': '10.0.0.11',
            'control.redis.port': '1234',
            'control.redis.password': 'passspam',
            'control.redis.db': '',
        }
        cfg = config.Config(conf_dict=local_conf)

        result = cfg.get_database(override='control')

        self.assertEqual(result, 'db_handle')
        mock_initialize.assert_called_once_with({
            'host': '10.0.0.11',
            'port': '1234',
            'password': 'passspam',
        })
        self.assertEqual(cfg._config, {
            None: {
                'status': '413 Request Entity Too Large',
            },
            'redis': {
                'host': '10.0.0.1',
                'password': 'spampass',
                'db': '3',
            },
            'control': {
                'host': '10.0.0.2',
                'redis.host': '10.0.0.11',
                'redis.password': 'passspam',
                'redis.port': '1234',
                'redis.db': '',
            },
        })

    def test_to_bool_integers(self):
        self.assertEqual(config.Config.to_bool('0'), False)
        self.assertEqual(config.Config.to_bool('1'), True)
        self.assertEqual(config.Config.to_bool('123412341234'), True)

    def test_to_bool_true(self):
        self.assertEqual(config.Config.to_bool('t'), True)
        self.assertEqual(config.Config.to_bool('true'), True)
        self.assertEqual(config.Config.to_bool('on'), True)
        self.assertEqual(config.Config.to_bool('y'), True)
        self.assertEqual(config.Config.to_bool('yes'), True)

    def test_to_bool_false(self):
        self.assertEqual(config.Config.to_bool('f'), False)
        self.assertEqual(config.Config.to_bool('false'), False)
        self.assertEqual(config.Config.to_bool('off'), False)
        self.assertEqual(config.Config.to_bool('n'), False)
        self.assertEqual(config.Config.to_bool('no'), False)

    def test_to_bool_invalid(self):
        self.assertRaises(ValueError, config.Config.to_bool, 'invalid')

    def test_to_bool_invalid_noraise(self):
        self.assertEqual(config.Config.to_bool('invalid', False), False)
