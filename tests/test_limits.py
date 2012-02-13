import time

from turnstile import limits

import tests


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
        self.assertEqual(limits.get_unit_value(3133.7), 3133.7)

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
        bucket = limits.Bucket.hydrate('db', 'limit', 'key', bucket_dict)

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

        limit = FakeLimit(cost=10.0, unit_value=100.0)
        bucket = limits.Bucket('db', limit, 'key')
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    def test_delay_expired(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100.0)
        bucket = limits.Bucket('db', limit, 'key', last=999990.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 10.0)

    def test_delay_overlap(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100.0)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=10.0)
        result = bucket.delay({})

        self.assertEqual(result, None)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000000.0)
        self.assertEqual(bucket.level, 15.0)

    def test_delay_overlimit(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100.0)
        bucket = limits.Bucket('db', limit, 'key', last=999995.0, level=100.0)
        result = bucket.delay({})

        self.assertEqual(result, 5.0)
        self.assertEqual(bucket.last, 1000000.0)
        self.assertEqual(bucket.next, 1000005.0)
        self.assertEqual(bucket.level, 95.0)

    def test_delay_undereps(self):
        self.stubs.Set(time, 'time', lambda: 1000000.0)

        limit = FakeLimit(cost=10.0, unit_value=100.0)
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
    attrs = set(['test_attr'])
    skip = set(['test_skip'])


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
        base_attrs = set(['uri', 'value', 'unit', 'verbs', 'requirements',
                          'continue_scan'])

        self.assertEqual(limits.Limit.attrs, base_attrs)
        self.assertEqual(LimitTest1.attrs, base_attrs)
        self.assertEqual(LimitTest2.attrs, base_attrs | set(['test_attr']))

    def test_skip(self):
        base_skip = set(['limit'])

        self.assertEqual(limits.Limit.skip, base_skip)
        self.assertEqual(LimitTest1.skip, base_skip)
        self.assertEqual(LimitTest2.skip, base_skip | set(['test_skip']))


class TestLimit(tests.TestCase):
    imports = {
        'LimitTest1': LimitTest1,
        'LimitTest2': LimitTest2,
        'FakeLimit': FakeLimit,
        }

    def test_init_default(self):
        limit = limits.Limit('db', 'uri', 10, 'second')

        self.assertEqual(limit.db, 'db')
        self.assertEqual(limit.uri, 'uri')
        self.assertEqual(limit._value, 10)
        self.assertEqual(limit._unit, 1.0)
        self.assertEqual(limit.verbs, [])
        self.assertEqual(limit.requirements, {})
        self.assertEqual(limit.continue_scan, True)

    def test_init_bad_value(self):
        with self.assertRaises(ValueError):
            limit = limits.Limit('db', 'uri', 0, 1.0)

    def test_init_bad_unit(self):
        with self.assertRaises(ValueError):
            limit = limits.Limit('db', 'uri', 10, 0.0)

    def test_init_verbs(self):
        limit = limits.Limit('db', 'uri', 10, 1.0,
                             verbs=['get', 'PUT', 'Head'])

        self.assertEqual(limit.verbs, ['GET', 'PUT', 'HEAD'])

    def test_init_requirements(self):
        expected = dict(foo=r'\..*', bar=r'.\.*')
        limit = limits.Limit('db', 'uri', 10, 1.0, requirements=expected)

        self.assertEqual(limit.requirements, expected)

    def test_init_continue_scan(self):
        limit = limits.Limit('db', 'uri', 10, 1.0, continue_scan=False)

        self.assertEqual(limit.continue_scan, False)

    def test_hydrate(self):
        expected = dict(
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
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
            uri='uri',
            value=10,
            unit='second',
            verbs=['GET', 'PUT'],
            requirements=dict(foo=r'\..*', bar=r'.\.*'),
            continue_scan=False,
            )
        expected = dict(limit_class='tests.test_limits:LimitTest1')
        expected.update(exemplar)
        limit = LimitTest1('db', **exemplar)

        self.assertEqual(limit.dehydrate(), expected)
