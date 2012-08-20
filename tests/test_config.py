import ConfigParser

from turnstile import config
from turnstile import database

import tests


class FakeConfigParser(object):
    _cfg_options = dict(
        with_file={
            'turnstile': {
                'preprocess': 'foo:bar',
                },
            'redis': {
                'host': '10.0.0.1',
                },
            'control': {
                'channel': 'control_channel',
                'connection_pool.connection': 'FoobarConnection',
                },
            },
        explicit_file={
            'turnstile': {
                'status': '500 Internal Error',
                },
            'redis': {
                'password': 'foobar',
                },
            'control': {
                'channel': 'special_control',
                'connection_pool': 'FoobarConnectionPool',
                },
            },
        connection_conf={
            'connection': {
                'host': '127.0.0.1',
                'password': 'foobar',
                'control_channel': 'control_channel',
                'limits_key': 'limits_key',
                },
            },
        )

    def __init__(self):
        self.config = None

    def read(self, cfg_files):
        self.config = {}
        for f in cfg_files:
            for key, value in self._cfg_options[f].items():
                self.config.setdefault(key, {})
                self.config[key].update(value)

    def sections(self):
        return self.config.keys()

    def items(self, section):
        return self.config[section].items()


class TestConfig(tests.TestCase):
    def setUp(self):
        super(TestConfig, self).setUp()

        def fake_initialize(cfg):
            return cfg

        self.stubs.Set(database, 'initialize', fake_initialize)
        self.stubs.Set(ConfigParser, 'SafeConfigParser', FakeConfigParser)

    def test_init_empty(self):
        cfg = config.Config()

        self.assertEqual(cfg._config, {
                None: {
                    'status': '413 Request Entity Too Large',
                    },
                })

    def test_init_dict(self):
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

    def test_init_dict_with_file(self):
        local_conf = {
            'config': 'with_file',
            'redis.host': '127.0.0.1',
            'redis.password': 'spampass',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg._config, {
                None: {
                    'status': '413 Request Entity Too Large',
                    'preprocess': 'foo:bar',
                    'config': 'with_file',
                    },
                'redis': {
                    'host': '10.0.0.1',
                    'password': 'spampass',
                    },
                'control': {
                    'channel': 'control_channel',
                    'connection_pool.connection': 'FoobarConnection',
                    },
                })

    def test_init_file(self):
        cfg = config.Config(conf_file='explicit_file')

        self.assertEqual(cfg._config, {
                None: {
                    'status': '500 Internal Error',
                    },
                'redis': {
                    'password': 'foobar',
                    },
                'control': {
                    'channel': 'special_control',
                    'connection_pool': 'FoobarConnectionPool',
                    },
                })

    def test_init_both(self):
        local_conf = {
            'config': 'with_file',
            'redis.host': '127.0.0.1',
            'redis.password': 'spampass',
            }

        cfg = config.Config(conf_dict=local_conf, conf_file='explicit_file')

        self.assertEqual(cfg._config, {
                None: {
                    'status': '500 Internal Error',
                    'preprocess': 'foo:bar',
                    'config': 'with_file',
                    },
                'redis': {
                    'host': '10.0.0.1',
                    'password': 'foobar',
                    },
                'control': {
                    'channel': 'special_control',
                    'connection_pool': 'FoobarConnectionPool',
                    'connection_pool.connection': 'FoobarConnection',
                    },
                })

    def test_init_connection(self):
        cfg = config.Config(conf_file='connection_conf')

        self.assertEqual(cfg._config, {
                None: {
                    'status': '413 Request Entity Too Large',
                    },
                'redis': {
                    'host': '127.0.0.1',
                    'password': 'foobar',
                    },
                'control': {
                    'channel': 'control_channel',
                    'limits_key': 'limits_key',
                    },
                })

    def test_getitem_exists(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg['redis'], dict(host='10.0.0.1'))

    def test_getitem_notexists(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg['control'], {})

    def test_contains_exists(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertTrue('redis' in cfg)

    def test_contains_notexists(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertFalse('control' in cfg)

    def test_getattr_exists(self):
        cfg = config.Config()

        self.assertEqual(cfg.status, '413 Request Entity Too Large')

    def test_getattr_notexists(self):
        cfg = config.Config()

        with self.assertRaises(AttributeError):
            dummy = cfg.preprocess

    def test_get_exists(self):
        cfg = config.Config()

        self.assertEqual(cfg.get('status'), '413 Request Entity Too Large')

    def test_get_notexists(self):
        cfg = config.Config()

        self.assertEqual(cfg.get('preprocess'), None)

    def test_get_notexists_default(self):
        cfg = config.Config()

        self.assertEqual(cfg.get('preprocess', 'default'), 'default')

    def test_get_database_no_override(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            'redis.password': 'foobar',
            'control.channel': 'control_channel',
            'control.redis.host': '127.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg.get_database(), {
                'host': '10.0.0.1',
                'password': 'foobar',
                })

    def test_get_database_override(self):
        local_conf = {
            'redis.host': '10.0.0.1',
            'redis.password': 'foobar',
            'control.channel': 'control_channel',
            'control.redis.host': '127.0.0.1',
            }

        cfg = config.Config(conf_dict=local_conf)

        self.assertEqual(cfg.get_database('control'), {
                'host': '127.0.0.1',
                'password': 'foobar',
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
