import time
import uuid

from turnstile import limits

import tests


class TestEncodeDecode(tests.TestCase):
    def test_encode(self):
        self.assertEqual(limits._encode('this is a test'),
                         '"this is a test"')
        self.assertEqual(limits._encode(123), '123')
        self.assertEqual(limits._encode("don't / your %s."),
                         '"don\'t %2f your %25s."')
        self.assertEqual(limits._encode('you said "hello".'),
                         '"you said \\"hello\\"."')

    def test_decode(self):
        self.assertEqual(limits._decode('"this is a test"'),
                         'this is a test')
        self.assertEqual(limits._decode('123'), 123)
        self.assertEqual(limits._decode('"don\'t %2f your %25s."'),
                         "don't / your %s.")
        self.assertEqual(limits._decode('"you said \\"hello\\"."'),
                         'you said "hello".')


class TestUnits(tests.TestCase):
    def test_unit_value(self):
        for unit in ('second', 'seconds', 'secs', 'sec', 's'):
            self.assertEqual(limits.get_unit_value(unit), 1)

        for unit in ('minute', 'minutes', 'mins', 'min', 'm'):
            self.assertEqual(limits.get_unit_value(unit), 60)

        for unit in ('hour', 'hours', 'hrs', 'hr', 'h'):
            self.assertEqual(limits.get_unit_value(unit), 3600)

        for unit in ('day', 'days', 'd'):
            self.assertEqual(limits.get_unit_value(unit), 86400)

        self.assertEqual(limits.get_unit_value('31337'), 31337)
        self.assertEqual(limits.get_unit_value(31337), 31337)

        with self.assertRaises(TypeError):
            val = limits.get_unit_value(3133.7)

        with self.assertRaises(KeyError):
            value = limits.get_unit_value('nosuchunit')

    def test_unit_name(self):
        for unit, value in [('second', 1), ('minute', 60), ('hour', 3600),
                            ('day', 86400)]:
            self.assertEqual(limits.get_unit_name(value), unit)

        self.assertEqual(limits.get_unit_name(31337), '31337')


class FakeLimit(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestBucket(tests.TestCase):
    def test_init(self):
        bucket = limits.Bucket('db', 'limit', 'key')

        self.assertEqual(bucket.db, 'db')
        self.assertEqual(bucket.limit, 'limit')
        self.assertEqual(bucket.key, 'key')
        self.assertEqual(bucket.last, None)
        self.assertEqual(bucket.next, None)
        self.assertEqual(bucket.level, 0.0)

    def test_hydrate(self):
        bucket_dict = dict(last=time.time() - 3600,
                           next=time.time(), level=0.5)
        bucket = limits.Bucket.hydrate('db', bucket_dict, 'limit', 'key')

        self.assertEqual(bucket.db, 'db')
        self.assertEqual(bucket.limit, 'limit')
        self.assertEqual(bucket.key, 'key')
        self.assertEqual(bucket.last, bucket_dict['last'])
        self.assertEqual(bucket.next, bucket_dict['next'])
        self.assertEqual(bucket.level, bucket_dict['level'])

    def test_dehydrate(self):
        bucket_dict = dict(last=time.time() - 3600,
                           next=time.time(), level=0.5)
        bucket = limits.Bucket('db', 'limit', 'key', **bucket_dict)
        newdict = bucket.dehydrate()

        self.assertEqual(bucket_dict, newdict)

    def test_delay_initial(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key')
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    def test_delay_expired(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999990.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    def test_delay_overlap(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 15.0)

    def test_delay_overlimit(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=100.0)
        result = bucket.delay({})

        self.assertEqual(result, 5.0)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000005.0)
        self.assertEqual(bucket.level, 95.0)

    def test_delay_undereps(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=95.1)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 100.1)

    def test_messages_empty(self):
        limit = FakeLimit(unit_value=1.0, value=10)
        bucket = limits.Bucket('db', limit, 'key')

        self.assertEqual(bucket.messages, 10)

    def test_messages_half(self):
        limit = FakeLimit(unit_value=1.0, value=10)
        bucket = limits.Bucket('db', limit, 'key', level=0.5)

        self.assertEqual(bucket.messages, 5)

    def test_messages_full(self):
        limit = FakeLimit(unit_value=1.0, value=10)
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
            default=''))

    def route(self, uri, route_args):
        route_args['route_add'] = 'LimitTest2'
        return 'mod_%s' % uri

    def filter(self, environ, params, unused):
        if 'defer' in environ:
            raise limits.DeferLimit
        environ['test.filter.unused'] = unused
        params['filter_add'] = 'LimitTest2_direct'
        return dict(additional='LimitTest2_additional')


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


class TestLimitMeta(tests.TestCase):
    def test_registry(self):
        expected = {
            'turnstile.limits:Limit': limits.Limit,
            'tests.test_limits:LimitTest1': LimitTest1,
            'tests.test_limits:LimitTest2': LimitTest2,
            }

        self.assertEqual(limits.LimitMeta._registry, expected)

    def test_full_name(self):
        self.assertEqual(LimitTest1._limit_full_name,
                         'tests.test_limits:LimitTest1')
        self.assertEqual(LimitTest2._limit_full_name,
                         'tests.test_limits:LimitTest2')

    def test_attrs(self):
        base_attrs = set(['uuid', 'uri', 'value', 'unit', 'verbs',
                          'requirements', 'queries', 'use', 'continue_scan'])

        self.assertEqual(set(limits.Limit.attrs.keys()), base_attrs)
        self.assertEqual(set(LimitTest1.attrs.keys()), base_attrs)
        self.assertEqual(set(LimitTest2.attrs.keys()),
                         base_attrs | set(['test_attr']))


class TestLimit(tests.TestCase):
    imports = {
        'LimitTest1': LimitTest1,
        'LimitTest2': LimitTest2,
        'FakeLimit': FakeLimit,
        }

    def test_init_default(self):
        self.stubs.Set(uuid, 'uuid4', lambda: 'fake_uuid')
        limit = limits.Limit('db', uri='uri', value=10, unit='second')

        self.assertEqual(limit.db, 'db')
        self.assertEqual(limit.uuid, 'fake_uuid')
        self.assertEqual(limit.uri, 'uri')
        self.assertEqual(limit._value, 10)
        self.assertEqual(limit._unit, 1)
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

    def test_hydrate(self):
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

        self.assertEqual(limit.__class__, limits.Limit)
        self.assertEqual(limit.db, 'db')
        for key, value in expected.items():
            self.assertEqual(getattr(limit, key), value)

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
        expected = dict(limit_class='tests.test_limits:LimitTest1')
        expected.update(exemplar)
        limit = LimitTest1('db', **exemplar)

        self.assertEqual(limit.dehydrate(), expected)

    def test_route_basic(self):
        mapper = FakeMapper()
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        limit._route(mapper)

        kwargs = dict(conditions=dict(function=limit._filter))
        self.assertEqual(mapper.routes, [(None, 'uri', kwargs)])

    def test_route_verbs(self):
        mapper = FakeMapper()
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             verbs=['get', 'post'])
        limit._route(mapper)

        kwargs = dict(conditions=dict(
                function=limit._filter,
                method=['GET', 'POST'],
                ))
        self.assertEqual(mapper.routes, [(None, 'uri', kwargs)])

    def test_route_requirements(self):
        mapper = FakeMapper()
        limit = limits.Limit('db', uri='uri', value=10, unit=1,
                             requirements=dict(foo=r'\..*', bar=r'.\.*'))
        limit._route(mapper)

        kwargs = dict(
            conditions=dict(function=limit._filter),
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            )
        self.assertEqual(mapper.routes, [(None, 'uri', kwargs)])

    def test_route_hook(self):
        mapper = FakeMapper()
        limit = LimitTest2('db', uri='uri', value=10, unit=1)
        limit._route(mapper)

        kwargs = dict(
            conditions=dict(function=limit._filter),
            route_add='LimitTest2',
            )
        self.assertEqual(mapper.routes, [(None, 'mod_uri', kwargs)])

    def test_key(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        params = dict(a=1, b=2, c=3, d=4, e=5, f=6)
        key = limit.key(params)

        self.assertEqual(key, 'bucket:%s/a=1/b=2/c=3/d=4/e=5/f=6' % limit.uuid)

    def test_decode(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        key = 'bucket:%s/a=1/b=2/c=3/d=4/e=5/f=6' % limit.uuid
        params = limit.decode(key)

        self.assertEqual(params, dict(a=1, b=2, c=3, d=4, e=5, f=6))

    def test_decode_badkey(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        key = 'badbucket:%s/a=1/b=2/c=3/d=4/e=5/f=6' % limit.uuid

        self.assertRaises(ValueError, limit.decode, key)

    def test_decode_baduuid(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        key = 'bucket:bad_uuid/a=1/b=2/c=3/d=4/e=5/f=6'

        self.assertRaises(ValueError, limit.decode, key)

    def test_decode_badpart(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        key = 'bucket:%s/a1/b=2/c=3/d=4/e=5/f=6' % limit.uuid

        self.assertRaises(ValueError, limit.decode, key)

    def test_decode_badvalue(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)
        key = 'bucket:%s/a=spam/b=2/c=3/d=4/e=5/f=6' % limit.uuid

        self.assertRaises(ValueError, limit.decode, key)

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

        self.assertEqual(limit._unit, 60)

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
            limit._unit = value
            self.assertEqual(limit.unit, unit)

        limit._unit = 31337
        self.assertEqual(limit.unit, '31337')

    def test_unit_set(self):
        limit = limits.Limit('db', uri='uri', value=10, unit=1)

        for unit in ('second', 'seconds', 'secs', 'sec', 's'):
            limit.unit = unit
            self.assertEqual(limit._unit, 1)

        for unit in ('minute', 'minutes', 'mins', 'min', 'm'):
            limit.unit = unit
            self.assertEqual(limit._unit, 60)

        for unit in ('hour', 'hours', 'hrs', 'hr', 'h'):
            limit.unit = unit
            self.assertEqual(limit._unit, 3600)

        for unit in ('day', 'days', 'd'):
            limit.unit = unit
            self.assertEqual(limit._unit, 86400)

        limit.unit = '31337'
        self.assertEqual(limit._unit, 31337)

        limit.unit = 31337
        self.assertEqual(limit._unit, 31337)

        with self.assertRaises(TypeError):
            limit.unit = 3133.7

        with self.assertRaises(KeyError):
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
