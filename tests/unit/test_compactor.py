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

from turnstile import compactor
from turnstile import config
from turnstile import control
from turnstile import database
from turnstile import limits
from turnstile import remote

from tests.unit import utils as test_utils


class TestVersionGreater(unittest2.TestCase):
    def test_version_equal(self):
        result = compactor.version_greater('1.2.3', '1.2.3')

        self.assertEqual(result, True)

    def test_version_greater_minor(self):
        result = compactor.version_greater('1.2', '1.2.3')

        self.assertEqual(result, True)

    def test_version_greater_major(self):
        result = compactor.version_greater('1.2', '1.3.1')

        self.assertEqual(result, True)

    def test_version_less(self):
        result = compactor.version_greater('1.2', '1.1.90')

        self.assertEqual(result, False)


class TestGetInt(unittest2.TestCase):
    def test_nonexistent(self):
        result = compactor.get_int({}, 'spam', 'default')

        self.assertEqual(result, 'default')

    def test_invalid(self):
        result = compactor.get_int(dict(spam='blah'), 'spam', 'default')

        self.assertEqual(result, 'default')

    def test_conversion(self):
        result = compactor.get_int(dict(spam='300'), 'spam', 'default')

        self.assertEqual(result, 300)


class TestGetBucketKey(unittest2.TestCase):
    @mock.patch.object(compactor.LOG, 'debug')
    @mock.patch.object(compactor, 'GetBucketKeyByLock',
                       return_value='by_lock')
    @mock.patch.object(compactor, 'GetBucketKeyByScript',
                       return_value='by_script')
    def test_factory_no_client(self, mock_GetBucketKeyByScript,
                               mock_GetBucketKeyByLock, mock_debug):
        db = mock.Mock(spec=[])
        result = compactor.GetBucketKey.factory('config', db)

        self.assertEqual(result, 'by_lock')
        self.assertFalse(mock_GetBucketKeyByScript.called)
        mock_GetBucketKeyByLock.assert_called_once_with('config', db)
        mock_debug.assert_called_once_with(
            "Redis client does not support register_script()")

    @mock.patch.object(compactor.LOG, 'debug')
    @mock.patch.object(compactor, 'GetBucketKeyByLock',
                       return_value='by_lock')
    @mock.patch.object(compactor, 'GetBucketKeyByScript',
                       return_value='by_script')
    def test_factory_no_server(self, mock_GetBucketKeyByScript,
                               mock_GetBucketKeyByLock, mock_debug):
        db = mock.Mock(spec=['info', 'register_script'],
                       **{'info.return_value': dict(redis_version='2.4')})
        result = compactor.GetBucketKey.factory('config', db)

        self.assertEqual(result, 'by_lock')
        self.assertFalse(mock_GetBucketKeyByScript.called)
        mock_GetBucketKeyByLock.assert_called_once_with('config', db)
        mock_debug.assert_called_once_with(
            "Redis server does not support register_script()")

    @mock.patch.object(compactor.LOG, 'debug')
    @mock.patch.object(compactor, 'GetBucketKeyByLock',
                       return_value='by_lock')
    @mock.patch.object(compactor, 'GetBucketKeyByScript',
                       return_value='by_script')
    def test_factory_with_script(self, mock_GetBucketKeyByScript,
                                 mock_GetBucketKeyByLock, mock_debug):
        db = mock.Mock(spec=['info', 'register_script'],
                       **{'info.return_value': dict(redis_version='2.6')})
        result = compactor.GetBucketKey.factory('config', db)

        self.assertEqual(result, 'by_script')
        self.assertFalse(mock_GetBucketKeyByLock.called)
        mock_GetBucketKeyByScript.assert_called_once_with('config', db)
        mock_debug.assert_called_once_with(
            "Redis server supports register_script()")

    def test_init(self):
        gbk = compactor.GetBucketKey({}, 'db')

        self.assertEqual(gbk.db, 'db')
        self.assertEqual(gbk.key, 'compactor')
        self.assertEqual(gbk.max_age, 600)
        self.assertEqual(gbk.min_age, 30)
        self.assertEqual(gbk.idle_sleep, 5)

    def test_init_altconf(self):
        gbk = compactor.GetBucketKey({
            'compactor_key': 'alt_compactor',
            'max_age': '60',
            'min_age': '5',
            'sleep': '1',
        }, 'db')

        self.assertEqual(gbk.db, 'db')
        self.assertEqual(gbk.key, 'alt_compactor')
        self.assertEqual(gbk.max_age, 60)
        self.assertEqual(gbk.min_age, 5)
        self.assertEqual(gbk.idle_sleep, 1)

    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(5))
    @mock.patch('time.sleep')
    @mock.patch.object(compactor.LOG, 'debug')
    @mock.patch.object(compactor.GetBucketKey, 'get', side_effect=[
        None,
        None,
        'bucket1',
        'bucket2',
    ])
    def test_call(self, mock_get, mock_debug, mock_sleep, mock_time):
        db = mock.Mock()
        gbk = compactor.GetBucketKey({}, db)

        result = gbk()

        self.assertEqual(result, 'bucket1')
        self.assertEqual(mock_time.call_count, 3)
        db.zrembyscore.assert_has_calls([
            mock.call('compactor', 0, 999400.0),
            mock.call('compactor', 0, 999405.0),
            mock.call('compactor', 0, 999410.0),
        ])
        self.assertEqual(db.zrembyscore.call_count, 3)
        mock_get.assert_has_calls([
            mock.call(1000000.0),
            mock.call(1000005.0),
            mock.call(1000010.0),
        ])
        self.assertEqual(mock_get.call_count, 3)
        mock_debug.assert_has_calls([
            mock.call("No buckets to compact; sleeping for 5 seconds"),
            mock.call("No buckets to compact; sleeping for 5 seconds"),
            mock.call("Next bucket to compact: bucket1"),
        ])
        mock_sleep.assert_has_calls([
            mock.call(5),
            mock.call(5),
        ])
        self.assertEqual(mock_sleep.call_count, 2)


class TestGetBucketKeyByLock(unittest2.TestCase):
    @mock.patch.object(compactor.LOG, 'debug')
    def test_init(self, mock_debug):
        db = mock.Mock(**{'lock.return_value': 'lock_obj'})
        gbtbl = compactor.GetBucketKeyByLock({}, db)

        self.assertEqual(gbtbl.lock, 'lock_obj')
        db.lock.assert_called_once_with('compactor_lock', timeout=30)
        mock_debug.assert_called_once_with(
            "Using GetBucketKeyByLock as bucket key getter")

    @mock.patch.object(compactor.LOG, 'debug')
    def test_init_altconf(self, mock_debug):
        db = mock.Mock(**{'lock.return_value': 'lock_obj'})
        gbtbl = compactor.GetBucketKeyByLock({
            'compactor_lock': 'alt_lock',
            'compactor_timeout': '10',
        }, db)

        self.assertEqual(gbtbl.lock, 'lock_obj')
        db.lock.assert_called_once_with('alt_lock', timeout=10)
        mock_debug.assert_called_once_with(
            "Using GetBucketKeyByLock as bucket key getter")

    @mock.patch.object(compactor.LOG, 'debug')
    def test_get_empty(self, mock_debug):
        lock = mock.MagicMock()
        db = mock.Mock(**{
            'lock.return_value': lock,
            'zrangebyscore.return_value': [],
        })
        gbtbl = compactor.GetBucketKeyByLock({}, db)

        result = gbtbl.get(1000000.0)

        self.assertEqual(result, None)
        lock.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        db.zrangebyscore.assert_called_once_with(
            'compactor', 0, 999970.0, start=0, num=1)
        self.assertFalse(db.zrem.called)

    @mock.patch.object(compactor.LOG, 'debug')
    def test_get_nonempty(self, mock_debug):
        lock = mock.MagicMock()
        db = mock.Mock(**{
            'lock.return_value': lock,
            'zrangebyscore.return_value': ['bucket1'],
        })
        gbtbl = compactor.GetBucketKeyByLock({}, db)

        result = gbtbl.get(1000000.0)

        self.assertEqual(result, 'bucket1')
        lock.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        db.zrangebyscore.assert_called_once_with(
            'compactor', 0, 999970.0, start=0, num=1)
        db.zrem.assert_called_once_with('bucket1')


class TestGetBucketByScript(unittest2.TestCase):
    @mock.patch.object(compactor.LOG, 'debug')
    def test_init(self, mock_debug):
        db = mock.Mock(**{'register_script.return_value': 'script_handle'})
        gbtbs = compactor.GetBucketKeyByScript({}, db)

        self.assertEqual(gbtbs.script, 'script_handle')
        db.register_script.assert_called_once_with(mock.ANY)
        mock_debug.assert_called_once_with(
            "Using GetBucketKeyByScript as bucket key getter")

    @mock.patch.object(compactor.LOG, 'debug')
    def test_get_empty(self, mock_debug):
        script = mock.Mock(return_value=[])
        db = mock.Mock(**{'register_script.return_value': script})
        gbtbs = compactor.GetBucketKeyByScript({}, db)

        result = gbtbs.get(1000000.0)

        self.assertEqual(result, None)
        script.assert_called_once_with(keys=['compactor'], args=[999970.0])

    @mock.patch.object(compactor.LOG, 'debug')
    def test_get_nonempty(self, mock_debug):
        script = mock.Mock(return_value=['bucket1'])
        db = mock.Mock(**{'register_script.return_value': script})
        gbtbs = compactor.GetBucketKeyByScript({}, db)

        result = gbtbs.get(1000000.0)

        self.assertEqual(result, 'bucket1')
        script.assert_called_once_with(keys=['compactor'], args=[999970.0])


class TestLimitContainer(unittest2.TestCase):
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    def test_init_local(self, mock_RemoteControlDaemon, mock_ControlDaemon):
        conf = config.Config(conf_dict={})

        lc = compactor.LimitContainer(conf, 'db')

        self.assertEqual(lc.conf, conf)
        self.assertEqual(lc.db, 'db')
        self.assertEqual(lc.limits, [])
        self.assertEqual(lc.limit_map, {})
        self.assertEqual(lc.limit_sum, None)
        self.assertEqual(lc.control_daemon,
                         mock_ControlDaemon.return_value)

        self.assertFalse(mock_RemoteControlDaemon.called)
        mock_ControlDaemon.assert_has_calls([
            mock.call(lc, conf),
            mock.call().start(),
        ])

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    def test_init_remote(self, mock_RemoteControlDaemon, mock_ControlDaemon):
        conf = config.Config(conf_dict={'control.remote': 'yes'})

        lc = compactor.LimitContainer(conf, 'db')

        self.assertEqual(lc.conf, conf)
        self.assertEqual(lc.db, 'db')
        self.assertEqual(lc.limits, [])
        self.assertEqual(lc.limit_map, {})
        self.assertEqual(lc.limit_sum, None)
        self.assertEqual(lc.control_daemon,
                         mock_RemoteControlDaemon.return_value)

        self.assertFalse(mock_ControlDaemon.called)
        mock_RemoteControlDaemon.assert_has_calls([
            mock.call(lc, conf),
            mock.call().start(),
        ])

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(compactor.LimitContainer, 'recheck_limits')
    def test_getitem(self, mock_recheck_limits, mock_ControlDaemon):
        lc = compactor.LimitContainer(config.Config(), 'db')
        lc.limit_map = dict(uuid1='limit1', uuid2='limit2')

        self.assertEqual(lc['uuid1'], 'limit1')
        self.assertEqual(lc['uuid2'], 'limit2')

        mock_recheck_limits.assert_has_calls([mock.call(), mock.call()])

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(compactor.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(uuid='uuid1'),
        mock.Mock(uuid='uuid2'),
    ])
    def test_recheck_limits_basic(self, mock_limits_hydrate, mock_exception,
                                  mock_ControlDaemon, mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.return_value': ('new_sum', ['limit1', 'limit2']),
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        lc = compactor.LimitContainer(config.Config(), mock.Mock())
        lc.limits = [mock.Mock(uuid='old_uuid1'), mock.Mock(uuid='old_uuid2')]
        lc.limit_sum = 'old_sum'
        lc.limit_map = dict(old_uuid1=lc.limits[0], old_uuid2=lc.limits[1])

        lc.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        mock_limits_hydrate.assert_called_once_with(lc.db,
                                                    ['limit1', 'limit2'])
        self.assertEqual(lc.limits, mock_limits_hydrate.return_value)
        self.assertEqual(lc.limit_sum, 'new_sum')
        self.assertEqual(lc.limit_map, dict(
            uuid1=mock_limits_hydrate.return_value[0],
            uuid2=mock_limits_hydrate.return_value[1],
        ))
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)
        self.assertEqual(len(lc.db.method_calls), 0)

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(compactor.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(uuid='uuid1'),
        mock.Mock(uuid='uuid2'),
    ])
    def test_recheck_limits_unchanged(self, mock_limits_hydrate,
                                      mock_exception, mock_ControlDaemon,
                                      mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': control.NoChangeException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        lc = compactor.LimitContainer(config.Config(), mock.Mock())
        old_limits = [mock.Mock(uuid='old_uuid1'), mock.Mock(uuid='old_uuid2')]
        lc.limits = old_limits
        lc.limit_sum = 'old_sum'
        old_limit_map = dict(old_uuid1=old_limits[0], old_uuid2=old_limits[1])
        lc.limit_map = old_limit_map

        lc.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertEqual(lc.limits, old_limits)
        self.assertEqual(lc.limit_sum, 'old_sum')
        self.assertEqual(lc.limit_map, old_limit_map)
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)
        self.assertEqual(len(lc.db.method_calls), 0)

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(compactor.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(uuid='uuid1'),
        mock.Mock(uuid='uuid2'),
    ])
    def test_recheck_limits_exception(self, mock_limits_hydrate,
                                      mock_exception, mock_ControlDaemon,
                                      mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': test_utils.TestException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        lc = compactor.LimitContainer(config.Config(), mock.Mock())
        old_limits = [mock.Mock(uuid='old_uuid1'), mock.Mock(uuid='old_uuid2')]
        lc.limits = old_limits
        lc.limit_sum = 'old_sum'
        old_limit_map = dict(old_uuid1=old_limits[0], old_uuid2=old_limits[1])
        lc.limit_map = old_limit_map

        lc.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertEqual(lc.limits, old_limits)
        self.assertEqual(lc.limit_sum, 'old_sum')
        self.assertEqual(lc.limit_map, old_limit_map)
        mock_exception.assert_called_once_with("Could not load limits")
        mock_format_exc.assert_called_once_with()
        lc.db.assert_has_calls([
            mock.call.sadd('errors', 'Failed to load limits: <traceback>'),
            mock.call.publish('errors', 'Failed to load limits: <traceback>'),
        ])

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(compactor.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(),
        mock.Mock(),
    ])
    def test_recheck_limits_exception_altkeys(self, mock_limits_hydrate,
                                              mock_exception,
                                              mock_ControlDaemon,
                                              mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': test_utils.TestException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        lc = compactor.LimitContainer(config.Config(conf_dict={
            'control.errors_key': 'eset',
            'control.errors_channel': 'epub',
        }), mock.Mock())
        old_limits = [mock.Mock(uuid='old_uuid1'), mock.Mock(uuid='old_uuid2')]
        lc.limits = old_limits
        lc.limit_sum = 'old_sum'
        old_limit_map = dict(old_uuid1=old_limits[0], old_uuid2=old_limits[1])
        lc.limit_map = old_limit_map

        lc.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertEqual(lc.limits, old_limits)
        self.assertEqual(lc.limit_sum, 'old_sum')
        self.assertEqual(lc.limit_map, old_limit_map)
        mock_exception.assert_called_once_with("Could not load limits")
        mock_format_exc.assert_called_once_with()
        lc.db.assert_has_calls([
            mock.call.sadd('eset', 'Failed to load limits: <traceback>'),
            mock.call.publish('epub', 'Failed to load limits: <traceback>'),
        ])


class TestCompactBucket(unittest2.TestCase):
    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('uuid.uuid4', return_value='bucket_uuid')
    @mock.patch.object(limits, 'BucketLoader', return_value=mock.Mock(**{
        'bucket': mock.Mock(**{'dehydrate.return_value': 'bucket'}),
        'last_summarize_rec': 'last_record',
        'last_summarize_idx': 17,
    }))
    @mock.patch.object(compactor.LOG, 'warning')
    def test_normal(self, mock_warning, mock_BucketLoader, mock_uuid4,
                    mock_dumps):
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
            'linsert.return_value': 23,
        })
        limit = mock.Mock(bucket_class='bucket_class')

        compactor.compact_bucket(db, 'bucket_key', limit)

        db.assert_has_calls([
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.linsert('bucket_key', 'after', 'last_record',
                              dict(bucket='bucket', uuid='bucket_uuid')),
            mock.call.ltrim('bucket_key', 18, -1),
        ])
        self.assertEqual(len(db.method_calls), 3)
        mock_BucketLoader.assert_called_once_with(
            'bucket_class', db, limit, 'bucket_key', ['record1', 'record2'],
            stop_summarize=True)
        self.assertFalse(mock_warning.called)

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('uuid.uuid4', return_value='bucket_uuid')
    @mock.patch.object(limits, 'BucketLoader', return_value=mock.Mock(**{
        'bucket': mock.Mock(**{'dehydrate.return_value': 'bucket'}),
        'last_summarize_rec': 'last_record',
        'last_summarize_idx': 17,
    }))
    @mock.patch.object(compactor.LOG, 'warning')
    def test_insert_failure(self, mock_warning, mock_BucketLoader, mock_uuid4,
                            mock_dumps):
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
            'linsert.return_value': -1,
        })
        limit = mock.Mock(bucket_class='bucket_class')

        compactor.compact_bucket(db, 'bucket_key', limit)

        db.assert_has_calls([
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.linsert('bucket_key', 'after', 'last_record',
                              dict(bucket='bucket', uuid='bucket_uuid')),
        ])
        self.assertEqual(len(db.method_calls), 2)
        mock_BucketLoader.assert_called_once_with(
            'bucket_class', db, limit, 'bucket_key', ['record1', 'record2'],
            stop_summarize=True)
        mock_warning.assert_called_once_with(
            "Bucket compaction on bucket_key failed; will retry")


class TestCompactor(unittest2.TestCase):
    @mock.patch.object(limits.BucketKey, 'decode')
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket')
    @mock.patch.object(compactor, 'LOG')
    def test_compactor(self, mock_LOG, mock_compact_bucket,
                       mock_GetBucketKey_factory, mock_LimitContainer,
                       mock_BucketKey_decode):
        key = mock.MagicMock(version=2, uuid='limit_uuid')
        key.__str__.return_value = 'str(bucket_key)'
        mock_BucketKey_decode.side_effect = [key, test_utils.Halt]
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = dict(max_updates=30)

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with(
            dict(max_updates=30), 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        mock_compact_bucket.assert_called_once_with('db', key, 'limit')
        mock_LOG.assert_has_calls([
            mock.call.info("Compactor initialized"),
            mock.call.debug("Compacting bucket str(bucket_key)"),
            mock.call.debug("Finished compacting bucket str(bucket_key)"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 3)

    @mock.patch.object(limits.BucketKey, 'decode')
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket')
    @mock.patch.object(compactor, 'LOG')
    def test_compactor_disabled(self, mock_LOG, mock_compact_bucket,
                                mock_GetBucketKey_factory, mock_LimitContainer,
                                mock_BucketKey_decode):
        key = mock.MagicMock(version=2, uuid='limit_uuid')
        key.__str__.return_value = 'str(bucket_key)'
        mock_BucketKey_decode.side_effect = [key, test_utils.Halt]
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = {}

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with({}, 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        mock_compact_bucket.assert_called_once_with('db', key, 'limit')
        mock_LOG.assert_has_calls([
            mock.call.warning("Compaction is not enabled.  Enable it by "
                              "setting a positive integer value for "
                              "'compactor.max_updates' in the configuration."),
            mock.call.info("Compactor initialized"),
            mock.call.debug("Compacting bucket str(bucket_key)"),
            mock.call.debug("Finished compacting bucket str(bucket_key)"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 4)

    @mock.patch.object(limits.BucketKey, 'decode',
                       side_effect=[ValueError('bad key'), test_utils.Halt])
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket')
    @mock.patch.object(compactor, 'LOG')
    def test_compactor_badkey(self, mock_LOG, mock_compact_bucket,
                              mock_GetBucketKey_factory, mock_LimitContainer,
                              mock_BucketKey_decode):
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = dict(max_updates=30)

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with(
            dict(max_updates=30), 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        self.assertFalse(mock_compact_bucket.called)
        mock_LOG.assert_has_calls([
            mock.call.info("Compactor initialized"),
            mock.call.warning("Error interpreting bucket key: bad key"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(limits.BucketKey, 'decode')
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket')
    @mock.patch.object(compactor, 'LOG')
    def test_compactor_v1key(self, mock_LOG, mock_compact_bucket,
                             mock_GetBucketKey_factory, mock_LimitContainer,
                             mock_BucketKey_decode):
        key = mock.MagicMock(version=1, uuid='limit_uuid')
        key.__str__.return_value = 'str(bucket_key)'
        mock_BucketKey_decode.side_effect = [key, test_utils.Halt]
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = dict(max_updates=30)

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with(
            dict(max_updates=30), 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        self.assertFalse(mock_compact_bucket.called)
        mock_LOG.assert_has_calls([
            mock.call.info("Compactor initialized"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 1)

    @mock.patch.object(limits.BucketKey, 'decode')
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket')
    @mock.patch.object(compactor, 'LOG')
    def test_compactor_nolimit(self, mock_LOG, mock_compact_bucket,
                               mock_GetBucketKey_factory, mock_LimitContainer,
                               mock_BucketKey_decode):
        key = mock.MagicMock(version=2, uuid='other_uuid')
        key.__str__.return_value = 'str(bucket_key)'
        mock_BucketKey_decode.side_effect = [key, test_utils.Halt]
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = dict(max_updates=30)

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with(
            dict(max_updates=30), 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        self.assertFalse(mock_compact_bucket.called)
        mock_LOG.assert_has_calls([
            mock.call.info("Compactor initialized"),
            mock.call.warning("Unable to compact bucket for limit other_uuid"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(limits.BucketKey, 'decode')
    @mock.patch.object(compactor, 'LimitContainer', return_value={
        'limit_uuid': 'limit',
    })
    @mock.patch.object(compactor.GetBucketKey, 'factory',
                       return_value=mock.Mock(return_value='bucket_key'))
    @mock.patch.object(compactor, 'compact_bucket',
                       side_effect=test_utils.TestException)
    @mock.patch.object(compactor, 'LOG')
    def test_compactor_failed(self, mock_LOG, mock_compact_bucket,
                              mock_GetBucketKey_factory, mock_LimitContainer,
                              mock_BucketKey_decode):
        key = mock.MagicMock(version=2, uuid='limit_uuid')
        key.__str__.return_value = 'str(bucket_key)'
        mock_BucketKey_decode.side_effect = [key, test_utils.Halt]
        conf = mock.MagicMock(**{
            'get_database.return_value': 'db',
        })
        conf.__getitem__.return_value = dict(max_updates=30)

        self.assertRaises(test_utils.Halt, compactor.compactor, conf)

        conf.get_database.assert_called_once_with('compactor')
        mock_LimitContainer.assert_called_once_with(conf, 'db')
        mock_GetBucketKey_factory.assert_called_once_with(
            dict(max_updates=30), 'db')
        mock_GetBucketKey_factory.return_value.assert_has_calls([
            mock.call(),
            mock.call(),
        ])
        mock_BucketKey_decode.assert_has_calls([
            mock.call('bucket_key'),
            mock.call('bucket_key'),
        ])
        mock_compact_bucket.assert_called_once_with('db', key, 'limit')
        mock_LOG.assert_has_calls([
            mock.call.info("Compactor initialized"),
            mock.call.debug("Compacting bucket str(bucket_key)"),
            mock.call.exception("Failed to compact bucket str(bucket_key)"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 3)
