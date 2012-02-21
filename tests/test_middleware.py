from turnstile import database
from turnstile import middleware

import tests


def preproc1(environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc1')


def preproc2(environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc2')


def preproc3(environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc3')


class TestHeadersDict(tests.TestCase):
    def test_init_sequence(self):
        hd = middleware.HeadersDict([('Foo', 'value'), ('bAR', 'VALUE')])

        self.assertEqual(hd.headers, dict(foo='value', bar='VALUE'))

    def test_init_dict(self):
        hd = middleware.HeadersDict(dict(Foo='value', bAR='VALUE'))

        self.assertEqual(hd.headers, dict(foo='value', bar='VALUE'))

    def test_init_kwargs(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        self.assertEqual(hd.headers, dict(foo='value', bar='VALUE'))

    def test_get_item(self):
        hd = middleware.HeadersDict(Foo='value')

        self.assertEqual(hd['foo'], 'value')
        self.assertEqual(hd['Foo'], 'value')
        with self.assertRaises(KeyError):
            foo = hd['bar']

    def test_set_item(self):
        hd = middleware.HeadersDict(Foo='value')

        hd['fOO'] = 'bar'
        self.assertEqual(hd.headers, dict(foo='bar'))
        hd['bAr'] = 'blah'
        self.assertEqual(hd.headers, dict(foo='bar', bar='blah'))

    def test_del_item(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        del hd['fOO']
        self.assertEqual(hd.headers, dict(bar='VALUE'))
        del hd['bar']
        self.assertEqual(hd.headers, {})
        with self.assertRaises(KeyError):
            del hd['baz']

    def test_contains(self):
        hd = middleware.HeadersDict(Foo='value')

        self.assertTrue('foo' in hd)
        self.assertTrue('fOO' in hd)
        self.assertFalse('bAR' in hd)

    def test_iter(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(iter(hd)))
        self.assertEqual(result, ['bar', 'foo'])

    def test_len(self):
        hd = middleware.HeadersDict(Foo='value')

        self.assertEqual(len(hd), 1)
        hd['bAR'] = 'VALUE'
        self.assertEqual(len(hd), 2)

    def test_iterkeys(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.iterkeys()))
        self.assertEqual(result, ['bar', 'foo'])

    def test_iteritems(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.iteritems()))
        self.assertEqual(result, [('bar', 'VALUE'), ('foo', 'value')])

    def test_itervalues(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.itervalues()))
        self.assertEqual(result, ['VALUE', 'value'])

    def test_keys(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.keys()))
        self.assertEqual(result, ['bar', 'foo'])

    def test_items(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.items()))
        self.assertEqual(result, [('bar', 'VALUE'), ('foo', 'value')])

    def test_values(self):
        hd = middleware.HeadersDict(Foo='value', bAR='VALUE')

        result = sorted(list(hd.values()))
        self.assertEqual(result, ['VALUE', 'value'])


class TestTurnstileFilter(tests.TestCase):
    def test_filter_factory(self):
        self.stubs.Set(middleware, 'TurnstileMiddleware',
                       tests.GenericFakeClass)

        result = middleware.turnstile_filter({}, foo='value1', bar='value2')
        self.assertTrue(callable(result))

        obj = result('app')
        self.assertIsInstance(obj, tests.GenericFakeClass)
        self.assertEqual(obj.args, ('app', dict(foo='value1', bar='value2')))


class TestTurnstileMiddleware(tests.TestCase):
    imports = {
        'preproc1': preproc1,
        'preproc2': preproc2,
        'preproc3': preproc3,
        }

    def setUp(self):
        super(TestTurnstileMiddleware, self).setUp()
        self.stubs.Set(database, 'initialize', lambda mid, cfg: (mid, cfg))

    def test_init_basic(self):
        mid = middleware.TurnstileMiddleware('app', {})

        self.assertEqual(mid.app, 'app')
        self.assertEqual(mid.mapper, None)
        self.assertEqual(mid.config, {
                None: dict(status='413 Request Entity Too Large'),
                })
        self.assertEqual(mid.preprocessors, [])
        self.assertEqual(id(mid.db), id(mid))
        self.assertEqual(mid.control_daemon, {})

    def test_init_config(self):
        config = {
            'status': '404 Not Found',
            'preprocess': 'preproc1 preproc2 preproc3',
            'foo': 'top-level',
            'foo.bar': 'mid-level',
            'foo.bar.baz': 'bottom',
            'redis.host': 'example.com',
            }
        mid = middleware.TurnstileMiddleware('app', config)

        self.assertEqual(mid.app, 'app')
        self.assertEqual(mid.mapper, None)
        self.assertEqual(mid.config, {
                None: dict(
                    status='404 Not Found',
                    preprocess='preproc1 preproc2 preproc3',
                    foo='top-level',
                    ),
                'foo': {
                    'bar': 'mid-level',
                    'bar.baz': 'bottom',
                    },
                'redis': dict(
                    host='example.com',
                    )})
        self.assertEqual(mid.preprocessors, [preproc1, preproc2, preproc3])
        self.assertEqual(id(mid.db), id(mid))
        self.assertEqual(mid.control_daemon, dict(host='example.com'))
