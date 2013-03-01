import time
import uuid

import mock

from turnstile import limits

import tests


class FakeLimit(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class LimitTest1(limits.Limit):
    pass


class FakeMapper(object):
    def __init__(self):
        self.routes = []

    def connect(self, name, uri, **kwargs):
        self.routes.append((name, uri, kwargs))


class FakeBucket(object):
    def __init__(self, delay):
        self._params = None
        self._delay = delay

    def delay(self, params):
        self._params = params
        return self._delay


class FakeDatabase(object):
    def __init__(self, bucket):
        self.update = None
        self.bucket = bucket

    def safe_update(self, key, klass, update, *args):
        self.update = (key, klass, update, args)
        return update(self.bucket)


class TestLimit(tests.TestCase):
    imports = {
        'LimitTest1': LimitTest1,
        'LimitTest2': LimitTest2,
        'FakeLimit': FakeLimit,
        }

    def test_filter_basic(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param="test"' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, {})
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_queries_empty_env(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1,
                             queries=['query'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertEqual(environ, {})
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update, None)
        self.assertEqual(bucket._params, None)

    def test_filter_queries_missing(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1,
                             queries=['query'])
        environ = dict(QUERY_STRING='noquery=boofar')
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertEqual(environ, dict(QUERY_STRING='noquery=boofar'))
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update, None)
        self.assertEqual(bucket._params, None)

    def test_filter_queries(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             queries=['query'])
        environ = dict(QUERY_STRING='query=spam')
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param="test"' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, dict(QUERY_STRING='query=spam'))
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_use(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param2'])
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param2="ni"' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, {})
        self.assertEqual(params, dict(param1='spam', param2='ni'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_use_empty(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1)
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        key = 'bucket:%s' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, {})
        self.assertEqual(params, dict(param1='spam', param2='ni'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_defer(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = LimitTest2(db, uri='uri', value=10, unit=1, use=['param'])
        environ = dict(defer=True)
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertEqual(environ, dict(defer=True))
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update, None)
        self.assertEqual(bucket._params, None)

    def test_filter_hook(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = LimitTest2(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = ('bucket:%s/filter_add="LimitTest2_direct"/param="test"' %
               limit.uuid)

        self.assertEqual(result, False)
        self.assertEqual(environ, {
                'test.filter.unused': {},
                })
        self.assertEqual(params, dict(
                param='test',
                filter_add='LimitTest2_direct',
                additional='LimitTest2_additional',
                ))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_hook_use(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = LimitTest2(db, uri='uri', value=10, unit=1, use=['param2'])
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        key = ('bucket:%s/filter_add="LimitTest2_direct"/param2="ni"' %
               limit.uuid)

        self.assertEqual(result, False)
        self.assertEqual(environ, {
                'test.filter.unused': dict(param1='spam'),
                })
        self.assertEqual(params, dict(
                param1='spam',
                param2='ni',
                filter_add='LimitTest2_direct',
                additional='LimitTest2_additional',
                ))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_hook_use_empty(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = LimitTest2(db, uri='uri', value=10, unit=1)
        environ = {}
        params = dict(param1='spam', param2='ni')
        result = limit._filter(environ, params)

        key = 'bucket:%s/filter_add="LimitTest2_direct"' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, {
                'test.filter.unused': dict(param1='spam', param2='ni'),
                })
        self.assertEqual(params, dict(
                param1='spam',
                param2='ni',
                filter_add='LimitTest2_direct',
                additional='LimitTest2_additional',
                ))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_delay(self):
        bucket = FakeBucket(10)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'])
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param="test"' % limit.uuid

        self.assertEqual(result, False)
        self.assertEqual(environ, {
                'turnstile.delay': [(10, limit, bucket)],
                })
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_continue_defer(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = LimitTest2(db, uri='uri', value=10, unit=1, use=['param'],
                           continue_scan=False)
        environ = dict(defer=True)
        params = dict(param='test')
        result = limit._filter(environ, params)

        self.assertEqual(result, False)
        self.assertEqual(environ, dict(defer=True))
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update, None)
        self.assertEqual(bucket._params, None)

    def test_filter_continue_delay(self):
        bucket = FakeBucket(10)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             continue_scan=False)
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param="test"' % limit.uuid

        self.assertEqual(result, True)
        self.assertEqual(environ, {
                'turnstile.delay': [(10, limit, bucket)],
                })
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

    def test_filter_continue_no_delay(self):
        bucket = FakeBucket(None)
        db = FakeDatabase(bucket)
        limit = limits.Limit(db, uri='uri', value=10, unit=1, use=['param'],
                             continue_scan=False)
        environ = {}
        params = dict(param='test')
        result = limit._filter(environ, params)

        key = 'bucket:%s/param="test"' % limit.uuid

        self.assertEqual(result, True)
        self.assertEqual(environ, {})
        self.assertEqual(params, dict(param='test'))
        self.assertEqual(db.update[0], key)
        self.assertEqual(db.update[1], limits.Bucket)
        self.assertEqual(db.update[3], (limit, key))
        self.assertEqual(id(bucket._params), id(params))

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
