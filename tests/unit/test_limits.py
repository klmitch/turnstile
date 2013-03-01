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

from turnstile import limits
from turnstile import utils


class TestMakeUnits(unittest2.TestCase):
    def test_make_units(self):
        result = limits._make_units(
            (1, ('a', 'b', 'c')),
            (2, ('d', 'e')),
            (3, ('f',)),
        )

        self.assertEqual(result, {
            1: 'a',
            'a': 1,
            'b': 1,
            'c': 1,
            2: 'd',
            'd': 2,
            'e': 2,
            3: 'f',
            'f': 3,
        })


class TestTimeUnit(unittest2.TestCase):
    def test_time_unit(self):
        for unit in ('second', 'seconds', 'secs', 'sec', 's', '1', 1):
            result = limits.TimeUnit(unit)
            self.assertEqual(result.value, 1)
            self.assertEqual(str(result), 'second')
            self.assertEqual(int(result), 1)

        for unit in ('minute', 'minutes', 'mins', 'min', 'm', '60', 60):
            result = limits.TimeUnit(unit)
            self.assertEqual(result.value, 60)
            self.assertEqual(str(result), 'minute')
            self.assertEqual(int(result), 60)

        for unit in ('hour', 'hours', 'hrs', 'hr', 'h', '3600', 3600):
            result = limits.TimeUnit(unit)
            self.assertEqual(result.value, 3600)
            self.assertEqual(str(result), 'hour')
            self.assertEqual(int(result), 3600)

        for unit in ('day', 'days', 'd', '86400', 86400):
            result = limits.TimeUnit(unit)
            self.assertEqual(result.value, 86400)
            self.assertEqual(str(result), 'day')
            self.assertEqual(int(result), 86400)

        for unit in ('31337', 31337):
            result = limits.TimeUnit(unit)
            self.assertEqual(result.value, 31337)
            self.assertEqual(str(result), '31337')
            self.assertEqual(int(result), 31337)

        self.assertRaises(ValueError, limits.TimeUnit, 3133.7)
        self.assertRaises(ValueError, limits.TimeUnit, -31337)
        self.assertRaises(ValueError, limits.TimeUnit, '-31337')
        self.assertRaises(ValueError, limits.TimeUnit, 'nosuchunit')


class TestBucketKey(unittest2.TestCase):
    def test_part_encode(self):
        self.assertEqual(limits.BucketKey._encode('this is a test'),
                         '"this is a test"')
        self.assertEqual(limits.BucketKey._encode(123), '123')
        self.assertEqual(limits.BucketKey._encode("don't / your %s."),
                         '"don\'t %2f your %25s."')
        self.assertEqual(limits.BucketKey._encode('you said "hello".'),
                         '"you said \\"hello\\"."')

    def test_part_decode(self):
        self.assertEqual(limits.BucketKey._decode('"this is a test"'),
                         'this is a test')
        self.assertEqual(limits.BucketKey._decode('123'), 123)
        self.assertEqual(limits.BucketKey._decode('"don\'t %2f your %25s."'),
                         "don't / your %s.")
        self.assertEqual(limits.BucketKey._decode('"you said \\"hello\\"."'),
                         'you said "hello".')

    def test_init_noversion(self):
        self.assertRaises(ValueError, limits.BucketKey, 'fake_uuid', {},
                          version=-1)

    def test_key_version1_noparams(self):
        key = limits.BucketKey('fake_uuid', {}, version=1)

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, {})
        self.assertEqual(key.version, 1)
        self.assertEqual(key._cache, None)

        expected = 'bucket:fake_uuid'

        self.assertEqual(str(key), expected)
        self.assertEqual(key._cache, expected)

    def test_key_version1_withparams(self):
        key = limits.BucketKey('fake_uuid', dict(a=1, b="2"), version=1)

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, dict(a=1, b="2"))
        self.assertEqual(key.version, 1)
        self.assertEqual(key._cache, None)

        expected = 'bucket:fake_uuid/a=1/b="2"'

        self.assertEqual(str(key), expected)
        self.assertEqual(key._cache, expected)

    def test_key_version2_noparams(self):
        key = limits.BucketKey('fake_uuid', {})

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, {})
        self.assertEqual(key.version, 2)
        self.assertEqual(key._cache, None)

        expected = 'bucket_v2:fake_uuid'

        self.assertEqual(str(key), expected)
        self.assertEqual(key._cache, expected)

    def test_key_version2_withparams(self):
        key = limits.BucketKey('fake_uuid', dict(a=1, b="2"))

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, dict(a=1, b="2"))
        self.assertEqual(key.version, 2)
        self.assertEqual(key._cache, None)

        expected = 'bucket_v2:fake_uuid/a=1/b="2"'

        self.assertEqual(str(key), expected)
        self.assertEqual(key._cache, expected)

    def test_decode_unprefixed(self):
        self.assertRaises(ValueError, limits.BucketKey.decode, 'unprefixed')

    def test_decode_badversion(self):
        self.assertRaises(ValueError, limits.BucketKey.decode, 'bad:fake_uuid')

    def test_decode_version1_noparams(self):
        key = limits.BucketKey.decode('bucket:fake_uuid')

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, {})
        self.assertEqual(key.version, 1)

    def test_decode_version1_withparams(self):
        key = limits.BucketKey.decode('bucket:fake_uuid/a=1/b="2"')

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, dict(a=1, b="2"))
        self.assertEqual(key.version, 1)

    def test_decode_version1_badparams(self):
        self.assertRaises(ValueError, limits.BucketKey.decode,
                          'bucket:fake_uuid/a=1/b="2"/c')

    def test_decode_version2_noparams(self):
        key = limits.BucketKey.decode('bucket_v2:fake_uuid')

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, {})
        self.assertEqual(key.version, 2)

    def test_decode_version2_withparams(self):
        key = limits.BucketKey.decode('bucket_v2:fake_uuid/a=1/b="2"')

        self.assertEqual(key.uuid, 'fake_uuid')
        self.assertEqual(key.params, dict(a=1, b="2"))
        self.assertEqual(key.version, 2)

    def test_decode_version2_badparams(self):
        self.assertRaises(ValueError, limits.BucketKey.decode,
                          'bucket_v2:fake_uuid/a=1/b="2"/c')


class TestBucketLoader(unittest2.TestCase):
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_no_bucket_records(self, mock_loads):
        bucket_class = mock.Mock(return_value='bucket')
        records = []

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records)

        self.assertFalse(mock_loads.called)
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        self.assertEqual(loader.bucket, 'bucket')
        self.assertEqual(loader.updates, 0)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, False)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_one_bucket_record(self, mock_loads):
        bucket_class = mock.Mock(**{'hydrate.return_value': 'bucket'})
        records = [
            dict(bucket='a bucket'),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records)

        mock_loads.assert_called_once_with(records[0])
        bucket_class.hydrate.assert_called_once_with(
            'db', 'a bucket', 'limit', 'key')
        self.assertEqual(loader.bucket, 'bucket')
        self.assertEqual(loader.updates, 0)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, False)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_one_update_record(self, mock_loads):
        bucket = mock.Mock(**{'delay.return_value': None})
        bucket_class = mock.Mock(return_value=bucket)
        records = [
            dict(update=dict(params='params', time='time')),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records)

        mock_loads.assert_called_once_with(records[0])
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        bucket.delay.assert_called_once_with('params', 'time')
        self.assertEqual(loader.bucket, bucket)
        self.assertEqual(loader.updates, 1)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, False)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_multi_update_record(self, mock_loads):
        bucket = mock.Mock(**{'delay.return_value': None})
        bucket_class = mock.Mock(return_value=bucket)
        records = [
            dict(update=dict(params='params0', time='time0')),
            dict(update=dict(params='params1', time='time1')),
            dict(update=dict(params='params2', time='time2'), uuid='stop'),
            dict(bucket='a bucket'),
            dict(update=dict(params='params3', time='time3')),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records, stop_uuid='stop')

        mock_loads.assert_has_calls([mock.call(rec) for rec in records])
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        self.assertFalse(bucket_class.hydrate.called)
        bucket.delay.assert_has_calls([
            mock.call('params0', 'time0'),
            mock.call('params1', 'time1'),
            mock.call('params2', 'time2'),
        ])
        self.assertEqual(bucket.delay.call_count, 3)
        self.assertEqual(loader.bucket, bucket)
        self.assertEqual(loader.updates, 3)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, False)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_multi_update_record_traverse_summarize(self, mock_loads):
        bucket = mock.Mock(**{'delay.return_value': None})
        bucket_class = mock.Mock(return_value=bucket)
        records = [
            dict(update=dict(params='params0', time='time0')),
            dict(update=dict(params='params1', time='time1')),
            dict(summarize=True),
            dict(update=dict(params='params2', time='time2'), uuid='stop'),
            dict(bucket='a bucket'),
            dict(update=dict(params='params3', time='time3')),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records, stop_uuid='stop')

        mock_loads.assert_has_calls([mock.call(rec) for rec in records])
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        self.assertFalse(bucket_class.hydrate.called)
        bucket.delay.assert_has_calls([
            mock.call('params0', 'time0'),
            mock.call('params1', 'time1'),
            mock.call('params2', 'time2'),
        ])
        self.assertEqual(bucket.delay.call_count, 3)
        self.assertEqual(loader.bucket, bucket)
        self.assertEqual(loader.updates, 3)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, True)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_multi_update_record_no_traverse_summarize(self, mock_loads):
        bucket = mock.Mock(**{'delay.return_value': None})
        bucket_class = mock.Mock(return_value=bucket)
        records = [
            dict(update=dict(params='params0', time='time0')),
            dict(update=dict(params='params1', time='time1')),
            dict(update=dict(params='params2', time='time2'), uuid='stop'),
            dict(bucket='a bucket'),
            dict(summarize=True),
            dict(update=dict(params='params3', time='time3')),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records, stop_uuid='stop')

        mock_loads.assert_has_calls([mock.call(rec) for rec in records])
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        self.assertFalse(bucket_class.hydrate.called)
        bucket.delay.assert_has_calls([
            mock.call('params0', 'time0'),
            mock.call('params1', 'time1'),
            mock.call('params2', 'time2'),
        ])
        self.assertEqual(bucket.delay.call_count, 3)
        self.assertEqual(loader.bucket, bucket)
        self.assertEqual(loader.updates, 3)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, True)
        self.assertEqual(loader.last_summarize, None)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_read_multi_summarize(self, mock_loads):
        bucket = mock.Mock(**{'delay.return_value': None})
        bucket_class = mock.Mock(return_value=bucket)
        records = [
            dict(update=dict(params='params0', time='time0')),
            dict(summarize=True),
            dict(update=dict(params='params1', time='time1')),
            dict(summarize=True),
            dict(update=dict(params='params2', time='time2')),
            dict(summarize=True),
            dict(update=dict(params='params3', time='time3')),
        ]

        loader = limits.BucketLoader(bucket_class, 'db', 'limit', 'key',
                                     records, stop_summarize=True)

        mock_loads.assert_has_calls([mock.call(rec) for rec in records])
        bucket_class.assert_called_once_with('db', 'limit', 'key')
        self.assertFalse(bucket_class.hydrate.called)
        bucket.delay.assert_has_calls([
            mock.call('params0', 'time0'),
            mock.call('params1', 'time1'),
            mock.call('params2', 'time2'),
        ])
        self.assertEqual(bucket.delay.call_count, 3)
        self.assertEqual(loader.bucket, bucket)
        self.assertEqual(loader.updates, 3)
        self.assertEqual(loader.delay, None)
        self.assertEqual(loader.summarized, True)
        self.assertEqual(loader.last_summarize, 5)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    def test_need_summary(self, mock_loads):
        loader = limits.BucketLoader(mock.Mock(), 'db', 'limit', 'key', [])
        loader.updates = 5

        self.assertFalse(loader.need_summary(10))
        self.assertTrue(loader.need_summary(5))
        self.assertTrue(loader.need_summary(4))

        loader.summarized = True

        self.assertFalse(loader.need_summary(10))
        self.assertFalse(loader.need_summary(5))
        self.assertFalse(loader.need_summary(4))


class TestBucket(unittest2.TestCase):
    def test_init(self):
        bucket = limits.Bucket('db', 'limit', 'key')

        self.assertEqual(bucket.db, 'db')
        self.assertEqual(bucket.limit, 'limit')
        self.assertEqual(bucket.key, 'key')
        self.assertEqual(bucket.last, None)
        self.assertEqual(bucket.next, None)
        self.assertEqual(bucket.level, 0.0)

    def test_hydrate(self):
        bucket_dict = dict(last=1000000.0 - 3600,
                           next=1000000.0, level=0.5)
        bucket = limits.Bucket.hydrate('db', bucket_dict, 'limit', 'key')

        self.assertEqual(bucket.db, 'db')
        self.assertEqual(bucket.limit, 'limit')
        self.assertEqual(bucket.key, 'key')
        self.assertEqual(bucket.last, bucket_dict['last'])
        self.assertEqual(bucket.next, bucket_dict['next'])
        self.assertEqual(bucket.level, bucket_dict['level'])

    def test_dehydrate(self):
        bucket_dict = dict(last=1000000.0 - 3600,
                           next=1000000.0, level=0.5)
        bucket = limits.Bucket('db', 'limit', 'key', **bucket_dict)
        newdict = bucket.dehydrate()

        self.assertEqual(bucket_dict, newdict)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_initial(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key')
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_expired(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999990.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_overlap(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 15.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_overlimit(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=100.0)
        result = bucket.delay({})

        self.assertEqual(result, 5.0)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000005.0)
        self.assertEqual(bucket.level, 95.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_overlimit_withnow(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=1000000.0, level=100.0)
        result = bucket.delay({}, now=1000005.0)

        self.assertEqual(result, 5.0)
        self.assertEqual(bucket.last, 1000005.0)
        self.assertEqual(bucket.next, 1000010.0)
        self.assertEqual(bucket.level, 95.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_overlimit_withnow_timetravel(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=1000010.0, level=100.0)
        result = bucket.delay({}, now=1000005.0)

        self.assertEqual(result, 10.0)
        self.assertEqual(bucket.last, 1000010.0)
        self.assertEqual(bucket.next, 1000020.0)
        self.assertEqual(bucket.level, 100.0)

    @mock.patch('time.time', return_value=1000000.0)
    def test_delay_undereps(self, mock_time):
        limit = mock.Mock(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=95.1)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 100.1)

    def test_messages_empty(self):
        limit = mock.Mock(unit_value=1.0, value=10)
        bucket = limits.Bucket('db', limit, 'key')

        self.assertEqual(bucket.messages, 10)

    def test_messages_half(self):
        limit = mock.Mock(unit_value=1.0, value=10)
        bucket = limits.Bucket('db', limit, 'key', level=0.5)

        self.assertEqual(bucket.messages, 5)

    def test_messages_full(self):
        limit = mock.Mock(unit_value=1.0, value=10)
        bucket = limits.Bucket('db', limit, 'key', level=1.0)

        self.assertEqual(bucket.messages, 0)

    def test_expire(self):
        bucket = limits.Bucket('db', 'limit', 'key', last=1000000.2,
                               level=5.2)

        self.assertEqual(bucket.expire, 1000006)


class LimitTest1(limits.Limit):
    pass


class LimitTest2(limits.Limit):
    attrs = dict(test_attr=dict(
        desc='Test attribute.',
        type=(str,),
        default=''
    ))

    def route(self, uri, route_args):
        route_args['route_add'] = 'LimitTest2'
        return 'mod_%s' % uri

    def filter(self, environ, params, unused):
        if 'defer' in environ:
            raise limits.DeferLimit
        environ['test.filter.unused'] = unused
        params['filter_add'] = 'LimitTest2_direct'
        return dict(additional='LimitTest2_additional')


class TestLimitMeta(unittest2.TestCase):
    def test_registry(self):
        expected = {
            'turnstile.limits:Limit': limits.Limit,
            'tests.unit.test_limits:LimitTest1': LimitTest1,
            'tests.unit.test_limits:LimitTest2': LimitTest2,
        }

        self.assertEqual(limits.LimitMeta._registry, expected)

    def test_full_name(self):
        self.assertEqual(LimitTest1._limit_full_name,
                         'tests.unit.test_limits:LimitTest1')
        self.assertEqual(LimitTest2._limit_full_name,
                         'tests.unit.test_limits:LimitTest2')

    def test_attrs(self):
        base_attrs = set(['uuid', 'uri', 'value', 'unit', 'verbs',
                          'requirements', 'queries', 'use', 'continue_scan'])

        self.assertEqual(set(limits.Limit.attrs.keys()), base_attrs)
        self.assertEqual(set(LimitTest1.attrs.keys()), base_attrs)
        self.assertEqual(set(LimitTest2.attrs.keys()),
                         base_attrs | set(['test_attr']))


class TestLimit(unittest2.TestCase):
    @mock.patch('uuid.uuid4', return_value='fake_uuid')
    def test_init_default(self, mock_uuid4):
        limit = limits.Limit('db', uri='uri', value=10, unit='second')

        self.assertEqual(limit.db, 'db')
        self.assertEqual(limit.uuid, 'fake_uuid')
        self.assertEqual(limit.uri, 'uri')
        self.assertEqual(limit._value, 10)
        self.assertEqual(int(limit._unit), 1)
        self.assertEqual(limit.verbs, [])
        self.assertEqual(limit.requirements, {})
        self.assertEqual(limit.use, [])
        self.assertEqual(limit.continue_scan, True)

    def test_init_uuid(self):
        limit1 = limits.Limit('db', uri='uri', value=10, unit='second')
        limit2 = limits.Limit('db', uri='uri', value=10, unit='second')

        self.assertNotEqual(limit1.uuid, limit2.uuid)

    def test_init_missing_value(self):
        with self.assertRaises(TypeError):
            limit = limits.Limit('db')

    def test_init_bad_value(self):
        with self.assertRaises(ValueError):
            limit = limits.Limit('db', uri='uri', value=0, unit=1)

    def test_init_bad_unit(self):
        with self.assertRaises(ValueError):
            limit = limits.Limit('db', uri='uri', value=10, unit=0)

    def test_init_verbs(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             verbs=['get', 'PUT', 'Head'])

        self.assertEqual(limit.verbs, ['GET', 'PUT', 'HEAD'])

    def test_init_requirements(self):
        expected = dict(foo=r'\..*', bar=r'.\.*')
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             requirements=expected)

        self.assertEqual(limit.requirements, expected)

    def test_init_use(self):
        expected = ['spam', 'ni']
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             use=expected)

        self.assertEqual(limit.use, expected)

    def test_init_continue_scan(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             continue_scan=False)

        self.assertEqual(limit.continue_scan, False)

    @mock.patch('uuid.uuid4', return_value='fake_uuid')
    def test_repr(self, mock_uuid4):
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             verbs=['GET', 'PUT'],
                             requirements=dict(foo=r'\..*', bar=r'.\.*'),
                             use=['baz', 'quux'], continue_scan=False)

        self.assertEqual(repr(limit), "<turnstile.limits:Limit "
                         "continue_scan=False queries=[] "
                         "requirements={bar='.\\\\.*', foo='\\\\..*'} "
                         "unit='second' uri='uri' use=['baz', 'quux'] "
                         "uuid='fake_uuid' value=10 verbs=['GET', 'PUT'] "
                         "at 0x%x>" % id(limit))

    @mock.patch.object(utils, 'import_class')
    def test_hydrate_from_registry(self, mock_import_class):
        expected = dict(
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            use=['baz'],
            continue_scan=False,
        )
        exemplar = dict(limit_class='turnstile.limits:Limit')
        exemplar.update(expected)
        limit = limits.Limit.hydrate('db', exemplar)

        self.assertFalse(mock_import_class.called)
        self.assertEqual(limit.__class__, limits.Limit)
        self.assertEqual(limit.db, 'db')
        for key, value in expected.items():
            self.assertEqual(getattr(limit, key), value)

    @mock.patch.dict(limits.LimitMeta._registry)
    @mock.patch.object(utils, 'import_class')
    def test_hydrate_no_match(self, mock_import_class):
        expected = dict(
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            use=['baz'],
            continue_scan=False,
        )
        exemplar = dict(limit_class='no.such:Limit')
        exemplar.update(expected)
        limit = limits.Limit.hydrate('db', exemplar)

        mock_import_class.assert_called_once_with(
            'no.such:Limit')
        self.assertEqual(limit, None)

    @mock.patch.dict(limits.LimitMeta._registry)
    @mock.patch.object(utils, 'import_class', side_effect=ImportError)
    def test_hydrate_import_error(self, mock_import_class):
        expected = dict(
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            use=['baz'],
            continue_scan=False,
        )
        exemplar = dict(limit_class='no.such:Limit')
        exemplar.update(expected)
        limit = limits.Limit.hydrate('db', exemplar)

        mock_import_class.assert_called_once_with(
            'no.such:Limit')
        self.assertEqual(limit, None)

    def test_dehydrate(self):
        exemplar = dict(
            uuid='fake_uuid',
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            queries=['spam'],
            use=['baz'],
            continue_scan=False,
        )
        expected = dict(limit_class='tests.unit.test_limits:LimitTest1')
        expected.update(exemplar)
        limit = LimitTest1('db', **exemplar)

        self.assertEqual(limit.dehydrate(), expected)

    @mock.patch.object(limits.Limit, 'route', return_value='xlated_uri')
    def test_route_basic(self, mock_route):
        mapper = mock.Mock()
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit._route(mapper)

        mock_route.assert_called_once_with(
            'uri', dict(conditions=dict(function=limit._filter)))
        mapper.connect.assert_called_once_with(
            None, 'xlated_uri', conditions=dict(function=limit._filter))

    @mock.patch.object(limits.Limit, 'route', return_value='xlated_uri')
    def test_route_verbs(self, mock_route):
        mapper = mock.Mock()
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             verbs=['get', 'post'])
        limit._route(mapper)

        kwargs = dict(conditions=dict(
            function=limit._filter,
            method=['GET', 'POST'],
        ))
        mock_route.assert_called_once_with('uri', kwargs)
        mapper.connect.assert_called_once_with(
            None, 'xlated_uri', **kwargs)

    @mock.patch.object(limits.Limit, 'route', return_value='xlated_uri')
    def test_route_requirements(self, mock_route):
        mapper = mock.Mock()
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             requirements=dict(foo=r'\..*', bar=r'.\.*'))
        limit._route(mapper)

        kwargs = dict(
            conditions=dict(function=limit._filter),
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
        )
        mock_route.assert_called_once_with('uri', kwargs)
        mapper.connect.assert_called_once_with(
            None, 'xlated_uri', **kwargs)

    def test_route_hook(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        result = limit.route('uri', {})

        self.assertEqual(result, 'uri')

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(limits.BucketKey, 'decode',
                       return_value=mock.MagicMock(**{
                           'uuid': 'fake_uuid',
                           'version': 1,
                       }))
    @mock.patch.object(limits, 'BucketLoader',
                       return_value=mock.Mock(bucket='v2 bucket'))
    def test_load_string_v1(self, mock_BucketLoader, mock_decode, mock_loads):
        mock_decode.return_value.__str__.return_value = 'parsed_key'
        db = mock.Mock(**{
            'get.return_value': 'bucket data',
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        limit.bucket_class = mock.Mock(**{
            'hydrate.return_value': 'v1 bucket',
        })

        result = limit.load('bucket_key')

        self.assertEqual(result, 'v1 bucket')
        mock_decode.assert_called_once_with('bucket_key')
        db.get.assert_called_once_with('parsed_key')
        mock_loads.assert_called_once_with('bucket data')
        limit.bucket_class.hydrate.assert_called_once_with(
            db, 'bucket data', limit, 'parsed_key')
        self.assertFalse(db.lrange.called)
        self.assertFalse(mock_BucketLoader.called)

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(limits.BucketKey, 'decode',
                       return_value=mock.MagicMock(**{
                           'uuid': 'fake_uuid',
                           'version': 2,
                       }))
    @mock.patch.object(limits, 'BucketLoader',
                       return_value=mock.Mock(bucket='v2 bucket'))
    def test_load_string_v2(self, mock_BucketLoader, mock_decode, mock_loads):
        mock_decode.return_value.__str__.return_value = 'parsed_key'
        db = mock.Mock(**{
            'get.return_value': 'bucket data',
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        limit.bucket_class = mock.Mock(**{
            'hydrate.return_value': 'v1 bucket',
        })

        result = limit.load('bucket_key')

        self.assertEqual(result, 'v2 bucket')
        mock_decode.assert_called_once_with('bucket_key')
        self.assertFalse(db.get.called)
        self.assertFalse(mock_loads.called)
        self.assertFalse(limit.bucket_class.hydrate.called)
        db.lrange.assert_called_once_with('parsed_key', 0, -1)
        mock_BucketLoader.assert_called_once_with(
            limit.bucket_class, db, limit, 'parsed_key',
            ['record1', 'record2'])

    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(limits.BucketKey, 'decode',
                       return_value=mock.MagicMock(**{
                           'uuid': 'fake_uuid',
                           'version': 1,
                       }))
    @mock.patch.object(limits, 'BucketLoader',
                       return_value=mock.Mock(bucket='v2 bucket'))
    def test_load_nonstr_uuid_mismatch(self, mock_BucketLoader, mock_decode,
                                       mock_loads):
        mock_decode.return_value.__str__.return_value = 'parsed_key'
        db = mock.Mock(**{
            'get.return_value': 'bucket data',
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        limit.bucket_class = mock.Mock(**{
            'hydrate.return_value': 'v1 bucket',
        })
        key = mock.MagicMock(**{
            'uuid': 'other_uuid',
            'version': 2,
        })
        key.__str__.return_value = 'parsed_other_key'

        self.assertRaises(ValueError, limit.load, key)

        self.assertFalse(mock_decode.called)
        self.assertFalse(db.get.called)
        self.assertFalse(mock_loads.called)
        self.assertFalse(limit.bucket_class.hydrate.called)
        self.assertFalse(db.lrange.called)
        self.assertFalse(mock_BucketLoader.called)

    @mock.patch.object(limits.BucketKey, 'decode',
                       return_value=mock.Mock(uuid='fake_uuid',
                                              params='params'))
    def test_decode(self, mock_decode):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        key = 'bucket:fake_uuid/a=1/b=2/c=3/d=4/e=5/f=6'
        params = limit.decode(key)

        mock_decode.assert_called_once_with(key)
        self.assertEqual(params, 'params')

    @mock.patch.object(limits.BucketKey, 'decode', side_effect=ValueError)
    def test_decode_badkey(self, mock_decode):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        key = 'bucket:fake_uuid/a=1/b=2/c=3/d=4/e=5/f=6'

        self.assertRaises(ValueError, limit.decode, key)
        mock_decode.assert_called_once_with(key)

    @mock.patch.object(limits.BucketKey, 'decode',
                       return_value=mock.Mock(uuid='other_uuid',
                                              params='params'))
    def test_decode_baduuid(self, mock_decode):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        key = 'bucket:fake_uuid/a=1/b=2/c=3/d=4/e=5/f=6'

        self.assertRaises(ValueError, limit.decode, key)
        mock_decode.assert_called_once_with(key)

    @mock.patch.object(limits, 'BucketKey', return_value=1234)
    def test_key(self, mock_BucketKey):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.uuid = 'fake_uuid'
        params = dict(a=1, b=2, c=3, d=4, e=5, f=6)
        key = limit.key(params)

        self.assertEqual(key, "1234")
        mock_BucketKey.assert_called_once_with('fake_uuid', params)

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_basic(self, mock_key, mock_filter, mock_BucketLoader,
                          mock_time, mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with({}, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_queries_notpresent(self, mock_key, mock_filter,
                                       mock_BucketLoader, mock_time,
                                       mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             queries=['query'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertFalse(mock_filter.called)
        self.assertFalse(mock_key.called)
        self.assertEqual(len(db.method_calls), 0)
        self.assertFalse(mock_BucketLoader.called)
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_queries_missing(self, mock_key, mock_filter,
                                    mock_BucketLoader, mock_time,
                                    mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             queries=['query'])
        environ = dict(QUERY_STRING='noquery=boofar')
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertFalse(mock_filter.called)
        self.assertFalse(mock_key.called)
        self.assertEqual(len(db.method_calls), 0)
        self.assertFalse(mock_BucketLoader.called)
        self.assertEqual(environ, dict(QUERY_STRING='noquery=boofar'))

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_queries(self, mock_key, mock_filter, mock_BucketLoader,
                            mock_time, mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             queries=['query'])
        environ = dict(QUERY_STRING='query=spam')
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(dict(QUERY_STRING='query=spam'),
                                            dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, dict(QUERY_STRING='query=spam'))

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter')
    @mock.patch.object(limits.Limit, 'key')
    def test_filter_use(self, mock_key, mock_filter, mock_BucketLoader,
                        mock_time, mock_uuid4, mock_dumps):
        filter_params = {}
        key_params = {}

        def filter_se(environ, params, unused):
            filter_params.update(params)
            return None

        def key_se(params):
            key_params.update(params)
            return 'bucket_key'

        mock_filter.side_effect = filter_se
        mock_key.side_effect = key_se
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param2'])
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with({}, params, dict(param1='spam'))
        self.assertEqual(filter_params, dict(param2='ni'))
        mock_key.assert_called_once_with(params)
        self.assertEqual(key_params, dict(param2='ni'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param1='spam', param2='ni'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter')
    @mock.patch.object(limits.Limit, 'key')
    def test_filter_use_empty(self, mock_key, mock_filter, mock_BucketLoader,
                              mock_time, mock_uuid4, mock_dumps):
        filter_params = {}
        key_params = {}

        def filter_se(environ, params, unused):
            filter_params.update(params)
            return None

        def key_se(params):
            key_params.update(params)
            return 'bucket_key'

        mock_filter.side_effect = filter_se
        mock_key.side_effect = key_se
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1)
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with({}, params,
                                            dict(param1='spam', param2='ni'))
        self.assertEqual(filter_params, {})
        mock_key.assert_called_once_with(params)
        self.assertEqual(key_params, {})

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param1='spam', param2='ni'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', side_effect=limits.DeferLimit)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_defer(self, mock_key, mock_filter, mock_BucketLoader,
                          mock_time, mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with({}, dict(param='test'), {})
        self.assertFalse(mock_key.called)
        self.assertEqual(len(db.method_calls), 0)
        self.assertFalse(mock_BucketLoader.called)
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter')
    @mock.patch.object(limits.Limit, 'key')
    def test_filter_hook(self, mock_key, mock_filter, mock_BucketLoader,
                         mock_time, mock_uuid4, mock_dumps):
        filter_params = {}
        key_params = {}

        def filter_se(environ, params, unused):
            filter_params.update(params)
            params['filter'] = 'add'
            return dict(additional='nothing')

        def key_se(params):
            key_params.update(params)
            return 'bucket_key'

        mock_filter.side_effect = filter_se
        mock_key.side_effect = key_se
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with({}, params, {})
        self.assertEqual(filter_params, dict(param='test'))
        mock_key.assert_called_once_with(params)
        self.assertEqual(key_params, dict(param='test', filter='add'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test', filter='add',
                               additional='nothing'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_delay(self, mock_key, mock_filter, mock_BucketLoader,
                          mock_time, mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=10,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {
            'turnstile.delay': [
                (10, limit, mock_BucketLoader.return_value.bucket),
            ],
        })

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_bucket_set(self, mock_key, mock_filter, mock_BucketLoader,
                               mock_time, mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {'turnstile.bucket_set': 'bucket_set'}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
            mock.call.zadd('bucket_set', 1000010, 'bucket_key')
        ])
        self.assertEqual(len(db.method_calls), 5)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertEqual(environ, {'turnstile.bucket_set': 'bucket_set'})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_compactor_noupdates(self, mock_key, mock_filter,
                                        mock_BucketLoader, mock_time,
                                        mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {'turnstile.conf': dict(compactor={})}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertFalse(mock_BucketLoader.return_value.need_summary.called)
        self.assertEqual(environ, {'turnstile.conf': dict(compactor={})})

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_compactor_badupdates(self, mock_key, mock_filter,
                                         mock_BucketLoader, mock_time,
                                         mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(
            delay=None,
            bucket=mock.Mock(expire=1000010),
        )
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {'turnstile.conf': dict(compactor=dict(max_updates='foo'))}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        self.assertFalse(mock_BucketLoader.return_value.need_summary.called)
        self.assertEqual(environ, {
            'turnstile.conf': dict(compactor=dict(max_updates='foo')),
        })

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.0)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_compactor_unneeded(self, mock_key, mock_filter,
                                       mock_BucketLoader, mock_time,
                                       mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(**{
            'delay': None,
            'bucket': mock.Mock(expire=1000010),
            'need_summary.return_value': False,
        })
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {'turnstile.conf': dict(compactor=dict(max_updates='10'))}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.0,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
        ])
        self.assertEqual(len(db.method_calls), 4)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        mock_BucketLoader.return_value.need_summary.assert_called_once_with(10)
        self.assertEqual(environ, {
            'turnstile.conf': dict(compactor=dict(max_updates='10')),
        })

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.1)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_compactor_needed(self, mock_key, mock_filter,
                                     mock_BucketLoader, mock_time,
                                     mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(**{
            'delay': None,
            'bucket': mock.Mock(expire=1000010),
            'need_summary.return_value': True,
        })
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {'turnstile.conf': dict(compactor=dict(max_updates='10'))}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.1,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
            mock.call.zadd('compactor', 1000001, 'bucket_key'),
        ])
        self.assertEqual(len(db.method_calls), 5)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        mock_BucketLoader.return_value.need_summary.assert_called_once_with(10)
        self.assertEqual(environ, {
            'turnstile.conf': dict(compactor=dict(max_updates='10')),
        })

    @mock.patch('msgpack.dumps', side_effect=lambda x: x)
    @mock.patch('time.time', return_value=1000000.1)
    @mock.patch('uuid.uuid4', return_value='update_uuid')
    @mock.patch.object(limits, 'BucketLoader')
    @mock.patch.object(limits.Limit, 'filter', return_value=None)
    @mock.patch.object(limits.Limit, 'key', return_value='bucket_key')
    def test_filter_compactor_needed_altkey(self, mock_key, mock_filter,
                                            mock_BucketLoader, mock_time,
                                            mock_uuid4, mock_dumps):
        mock_BucketLoader.return_value = mock.Mock(**{
            'delay': None,
            'bucket': mock.Mock(expire=1000010),
            'need_summary.return_value': True,
        })
        db = mock.Mock(**{
            'lrange.return_value': ['record1', 'record2'],
        })
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {
            'turnstile.conf': {
                'compactor': dict(max_updates='10', compactor_key='alt_key'),
            },
        }
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        mock_filter.assert_called_once_with(environ, dict(param='test'), {})
        mock_key.assert_called_once_with(dict(param='test'))

        update_record = {
            'uuid': 'update_uuid',
            'update': {
                'params': dict(param='test'),
                'time': 1000000.1,
            },
        }

        db.assert_has_calls([
            mock.call.expire('bucket_key', 60),
            mock.call.rpush('bucket_key', update_record),
            mock.call.lrange('bucket_key', 0, -1),
            mock.call.expireat('bucket_key', 1000010),
            mock.call.zadd('alt_key', 1000001, 'bucket_key'),
        ])
        self.assertEqual(len(db.method_calls), 5)
        mock_BucketLoader.assert_called_once_with(
            limits.Bucket, db, limit, 'bucket_key', ['record1', 'record2'])
        mock_BucketLoader.return_value.need_summary.assert_called_once_with(10)
        self.assertEqual(environ, {
            'turnstile.conf': {
                'compactor': dict(max_updates='10', compactor_key='alt_key'),
            },
        })

    def test_format(self):
        expected = ("This request was rate-limited.  Please retry your "
                    "request after 1970-01-12T13:46:40Z.")
        status = '413 Request Entity Too Large'
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        bucket = limits.Bucket('db', limit, 'key', next=1000000.0)
        headers = {}

        result_status, result_entity = limit.format(status, headers, {},
                                                    bucket, 123)

        self.assertEqual(result_status, status)
        self.assertEqual(result_entity, expected)
        self.assertEqual(headers, {'Content-Type': 'text/plain'})

    def test_value_get(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        self.assertEqual(limit.value, 10)

    def test_value_set(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.value = 20

        self.assertEqual(limit._value, 20)

    def test_value_set_zero(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        with self.assertRaises(ValueError):
            limit.value = 0

    def test_value_set_negative(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        with self.assertRaises(ValueError):
            limit.value = -1

    def test_unit_value_get(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        self.assertEqual(limit.unit_value, 1)

    def test_unit_value_set(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit.unit_value = 60

        self.assertEqual(int(limit._unit), 60)

    def test_unit_value_set_zero(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        with self.assertRaises(ValueError):
            limit.unit_value = 0

    def test_unit_value_set_negative(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        with self.assertRaises(ValueError):
            limit.unit_value = -1

    def test_unit_get(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        for unit, value in [('second', 1), ('minute', 60), ('hour', 3600),
                            ('day', 86400)]:
            limit.unit = value
            self.assertEqual(limit.unit, unit)

        limit.unit = 31337
        self.assertEqual(limit.unit, '31337')

    def test_unit_set(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        for unit in ('second', 'seconds', 'secs', 'sec', 's', '1', 1):
            limit.unit = unit
            self.assertEqual(int(limit._unit), 1)

        for unit in ('minute', 'minutes', 'mins', 'min', 'm', '60', 60):
            limit.unit = unit
            self.assertEqual(int(limit._unit), 60)

        for unit in ('hour', 'hours', 'hrs', 'hr', 'h', '3600', 3600):
            limit.unit = unit
            self.assertEqual(int(limit._unit), 3600)

        for unit in ('day', 'days', 'd', '86400', 86400):
            limit.unit = unit
            self.assertEqual(int(limit._unit), 86400)

        for unit in ('31337', 31337):
            limit.unit = unit
            self.assertEqual(int(limit._unit), 31337)

        with self.assertRaises(ValueError):
            limit.unit = 3133.7

        with self.assertRaises(ValueError):
            limit.unit = 'nosuchunit'

        with self.assertRaises(ValueError):
            limit.unit = '0'

        with self.assertRaises(ValueError):
            limit.unit = -1

    def test_cost(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        self.assertEqual(limit.cost, 0.1)

        limit.unit = 60
        self.assertEqual(limit.cost, 6.0)
