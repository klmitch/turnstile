import routes

from turnstile import control
from turnstile import database
from turnstile import middleware
from turnstile import remote

import tests
from tests import db_fixture


def preproc1(mid, environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc1')


def preproc2(mid, environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc2')


def preproc3(mid, environ):
    environ.setdefault('turnstile.preprocess', [])
    environ['turnstile.preprocess'].append('preproc3')


class FakeMiddleware(tests.GenericFakeClass):
    pass


class FakeCallMapper(object):
    def __init__(self, delay=None):
        self.delay = delay
        self.environ = None

    def routematch(self, environ):
        self.environ = environ
        if self.delay:
            self.environ['turnstile.delay'] = self.delay


class FakeLimit(object):
    def __init__(self, ident, headers=None):
        self.ident = ident
        self.headers = headers
        self.environ = None
        self.bucket = None
        self.delay = None

    def format(self, status, headers, environ, bucket, delay):
        self.environ = environ
        self.bucket = bucket
        self.delay = delay
        if self.headers:
            headers.update(self.headers)
        return status, "Fake Entity for limit %s" % self.ident


class FakeRecheckMapper(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeRecheckMapper, self).__init__(*args, **kwargs)

        self.routes = []


class FakeFailingRecheckMapper(FakeRecheckMapper):
    def __init__(self, *args, **kwargs):
        raise Exception("Fake-out")


class FakeControlDaemon(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeControlDaemon, self).__init__(*args, **kwargs)
        self._started = False

    def start(self):
        self._started = True


class FakeRemoteControlDaemon(FakeControlDaemon):
    pass


class Response(object):
    def __init__(self):
        self.status = None
        self.headers = None

    def start(self, status, headers):
        self.status = status
        self.headers = dict(headers)


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
    imports = {
        'FakeMiddleware': FakeMiddleware,
        }

    def test_filter_factory(self):
        self.stubs.Set(middleware, 'TurnstileMiddleware',
                       tests.GenericFakeClass)

        result = middleware.turnstile_filter({}, foo='value1', bar='value2')
        self.assertTrue(callable(result))

        obj = result('app')
        self.assertIsInstance(obj, tests.GenericFakeClass)
        self.assertEqual(obj.args, ('app', dict(foo='value1', bar='value2')))

    def test_filter_factory_alternate_middleware(self):
        result = middleware.turnstile_filter({}, foo='value1', bar='value2',
                                             turnstile='FakeMiddleware')
        self.assertTrue(callable(result))

        obj = result('app')
        self.assertIsInstance(obj, FakeMiddleware)
        self.assertEqual(obj.args, ('app', dict(
                    foo='value1',
                    bar='value2',
                    turnstile='FakeMiddleware',
                    )))


class TestTurnstileMiddleware(tests.TestCase):
    imports = {
        'preproc1': preproc1,
        'preproc2': preproc2,
        'preproc3': preproc3,
        }

    def setUp(self):
        super(TestTurnstileMiddleware, self).setUp()

        def fake_limits_hydrate(db, lims):
            return [db_fixture.FakeLimit.hydrate(db, lim) for lim in lims]

        self.stubs.Set(database, 'initialize', lambda cfg: cfg)
        self.stubs.Set(control, 'ControlDaemon', FakeControlDaemon)
        self.stubs.Set(remote, 'RemoteControlDaemon', FakeRemoteControlDaemon)
        self.stubs.Set(database, 'limits_hydrate', fake_limits_hydrate)

    def stub_recheck_limits(self):
        self.stubs.Set(middleware.TurnstileMiddleware, 'recheck_limits',
                       lambda *args: None)

    def stub_mapper(self, mapper=FakeRecheckMapper):
        self.stubs.Set(routes, 'Mapper', mapper)

    def test_init_basic(self):
        mid = middleware.TurnstileMiddleware('app', {})

        self.assertEqual(mid.app, 'app')
        self.assertEqual(mid.limits, [])
        self.assertEqual(mid.limit_sum, None)
        self.assertEqual(mid.mapper, None)
        self.assertEqual(mid.config, {
                None: dict(status='413 Request Entity Too Large'),
                })
        self.assertEqual(mid.preprocessors, [])
        self.assertEqual(mid.db, {})
        self.assertIsInstance(mid.control_daemon, tests.GenericFakeClass)
        self.assertEqual(mid.control_daemon.__class__, FakeControlDaemon)
        self.assertEqual(mid.control_daemon.args, (mid, mid.conf))
        self.assertEqual(mid.control_daemon._started, True)

    def test_init_config(self):
        config = {
            'status': '404 Not Found',
            'preprocess': 'preproc1 preproc2 preproc3',
            'foo': 'top-level',
            'foo.bar': 'mid-level',
            'foo.bar.baz': 'bottom',
            'redis.host': 'example.com',
            'control.channel': 'spam',
            'control.node_name': 'node1',
            'control.remote': 'on',
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
                    ),
                'control': dict(
                    channel='spam',
                    node_name='node1',
                    remote='on',
                    ),
                })
        self.assertEqual(mid.preprocessors, [preproc1, preproc2, preproc3])
        self.assertEqual(mid.db, dict(host='example.com'))
        self.assertIsInstance(mid.control_daemon, tests.GenericFakeClass)
        self.assertEqual(mid.control_daemon.__class__, FakeRemoteControlDaemon)
        self.assertEqual(mid.control_daemon.args, (mid, mid.conf))
        self.assertEqual(mid.control_daemon._started, True)

    def test_call_through(self):
        self.stub_recheck_limits()

        response = Response()
        environ = dict(test=True)

        def app(env, start_response):
            self.assertEqual(id(env), id(environ))
            self.assertEqual(start_response, response.start)

            return 'app called'

        mid = middleware.TurnstileMiddleware(app, {})
        mid.mapper = FakeCallMapper()
        result = mid(environ, response.start)

        self.assertEqual(id(mid.mapper.environ), id(environ))
        self.assertEqual(environ['test'], True)
        self.assertEqual(result, 'app called')
        self.assertEqual(response.status, None)
        self.assertEqual(response.headers, None)

    def test_call_preprocess(self):
        self.stub_recheck_limits()

        response = Response()
        environ = dict(test=True)

        def app(env, start_response):
            self.assertEqual(id(env), id(environ))
            self.assertEqual(start_response, response.start)

            return 'app called'

        mid = middleware.TurnstileMiddleware(app, dict(
                preprocess='preproc1 preproc2 preproc3',
                ))
        mid.mapper = FakeCallMapper()
        result = mid(environ, response.start)

        self.assertEqual(id(mid.mapper.environ), id(environ))
        self.assertEqual(environ['turnstile.preprocess'],
                         ['preproc1', 'preproc2', 'preproc3'])
        self.assertEqual(result, 'app called')
        self.assertEqual(response.status, None)
        self.assertEqual(response.headers, None)

    def test_call_limited(self):
        self.stub_recheck_limits()

        response = Response()
        environ = dict(test=True)
        delays = [
            (1.5, FakeLimit('limit1'), 'bucket1'),
            (3.4, FakeLimit('limit2'), 'bucket2'),
            (2.5, FakeLimit('limit3'), 'bucket3'),
            ]

        def app(env, start_response):
            self.assertTrue(False)

        mid = middleware.TurnstileMiddleware(app, {})
        mid.mapper = FakeCallMapper(delays)
        result = mid(environ, response.start)

        self.assertEqual(id(mid.mapper.environ), id(environ))
        self.assertEqual(environ['turnstile.delay'], delays)
        self.assertEqual(result, 'Fake Entity for limit limit2')
        self.assertEqual(response.status, '413 Request Entity Too Large')
        self.assertEqual(response.headers, {
                'retry-after': '4',
                })

        self.assertEqual(delays[0][1].environ, None)
        self.assertEqual(delays[0][1].bucket, None)
        self.assertEqual(id(delays[1][1].environ), id(environ))
        self.assertEqual(delays[1][1].bucket, 'bucket2')
        self.assertEqual(delays[2][1].environ, None)
        self.assertEqual(delays[2][1].bucket, None)

    def test_call_limited_alternate_status(self):
        self.stub_recheck_limits()

        response = Response()
        environ = dict(test=True)
        delays = [(3.4, FakeLimit('limit1'), 'bucket1')]

        def app(env, start_response):
            self.assertTrue(False)

        mid = middleware.TurnstileMiddleware(app, dict(status='404 Not Found'))
        mid.mapper = FakeCallMapper(delays)
        result = mid(environ, response.start)

        self.assertEqual(id(mid.mapper.environ), id(environ))
        self.assertEqual(environ['turnstile.delay'], delays)
        self.assertEqual(result, 'Fake Entity for limit limit1')
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.headers, {
                'retry-after': '4',
                })

        self.assertEqual(id(delays[0][1].environ), id(environ))
        self.assertEqual(delays[0][1].bucket, 'bucket1')

    def test_call_limited_extra_headers(self):
        self.stub_recheck_limits()

        response = Response()
        environ = dict(test=True)
        delays = [(3.4, FakeLimit('limit1', dict(test='foo')), 'bucket1')]

        def app(env, start_response):
            self.assertTrue(False)

        mid = middleware.TurnstileMiddleware(app, {})
        mid.mapper = FakeCallMapper(delays)
        result = mid(environ, response.start)

        self.assertEqual(id(mid.mapper.environ), id(environ))
        self.assertEqual(environ['turnstile.delay'], delays)
        self.assertEqual(result, 'Fake Entity for limit limit1')
        self.assertEqual(response.status, '413 Request Entity Too Large')
        self.assertEqual(response.headers, {
                'retry-after': '4',
                'test': 'foo',
                })

        self.assertEqual(id(delays[0][1].environ), id(environ))
        self.assertEqual(delays[0][1].bucket, 'bucket1')

    def test_recheck_limits_empty(self):
        self.stub_mapper()

        db = db_fixture.FakeDatabase()
        ld = db_fixture.FakeLimitData()
        mid = middleware.TurnstileMiddleware('app', {})
        mid._db = db
        mid.control_daemon.get_limits = lambda: ld
        mid.limits = None  # Check that these get updated
        mid.mapper = None
        mid.recheck_limits()

        self.assertEqual(mid.limits, [])
        self.assertEqual(mid.limit_sum, 0)
        self.assertIsInstance(mid.mapper, FakeRecheckMapper)
        self.assertEqual(mid.mapper.kwargs, dict(register=False))
        self.assertEqual(mid.mapper.routes, [])

    def test_recheck_limits_entries(self):
        self.stub_mapper()

        db = db_fixture.FakeDatabase()
        ld = db_fixture.FakeLimitData([dict(limit='limit1'),
                                       dict(limit='limit2')])
        mid = middleware.TurnstileMiddleware('app', {})
        mid._db = db
        mid.control_daemon.get_limits = lambda: ld
        mid.limits = None  # Check that these get updated
        mid.mapper = None
        mid.recheck_limits()

        self.assertEqual(len(mid.limits), 2)
        for idx, lim in enumerate(mid.limits):
            self.assertIsInstance(lim, db_fixture.FakeLimit)
            self.assertEqual(lim.args, (db,))
            self.assertEqual(lim.kwargs, dict(limit='limit%d' % (idx + 1)))
        self.assertEqual(mid.limit_sum, 2)
        self.assertIsInstance(mid.mapper, FakeRecheckMapper)
        self.assertEqual(mid.mapper.kwargs, dict(register=False))
        self.assertEqual(len(mid.mapper.routes), 2)
        for idx, route in enumerate(mid.mapper.routes):
            self.assertIsInstance(route, db_fixture.FakeLimit)
            self.assertEqual(route.args, (db,))
            self.assertEqual(route.kwargs, dict(limit='limit%d' % (idx + 1)))

    def test_recheck_limits_nochange(self):
        self.stub_mapper()

        db = db_fixture.FakeDatabase()
        ld = db_fixture.FakeLimitData([dict(limit='limit1'),
                                       dict(limit='limit2')])
        mid = middleware.TurnstileMiddleware('app', {})
        mid._db = db
        mid.control_daemon.get_limits = lambda: ld
        mid.limits = None  # Check that these don't get updated
        mid.mapper = None
        mid.limit_sum = ld.limit_sum
        mid.recheck_limits()

        self.assertEqual(mid.limits, None)
        self.assertEqual(mid.limit_sum, 2)
        self.assertEqual(mid.mapper, None)
        self.assertEqual(len(self.log_messages), 1)

    def test_recheck_limits_failure(self):
        self.stub_mapper(FakeFailingRecheckMapper)

        db = db_fixture.FakeDatabase()
        db._fakedb['errors'] = set()
        ld = db_fixture.FakeLimitData()
        mid = middleware.TurnstileMiddleware('app', {})
        mid._db = db
        mid.control_daemon.get_limits = lambda: ld
        mid.limits = None  # Check that these don't get updated
        mid.mapper = None
        mid.recheck_limits()

        self.assertEqual(mid.limits, None)
        self.assertEqual(mid.limit_sum, None)
        self.assertEqual(mid.mapper, None)
        self.assertEqual(len(self.log_messages), 2)
        self.assertTrue(self.log_messages[1].startswith(
                'Could not load limits'))
        self.assertEqual(db._actions[0][0], 'sadd')
        self.assertEqual(db._actions[0][1], 'errors')
        self.assertTrue(db._actions[0][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(db._actions[1][0], 'publish')
        self.assertEqual(db._actions[1][1], 'errors')
        self.assertTrue(db._actions[1][2].startswith(
                'Failed to load limits: '))

    def test_recheck_limits_failure_alternate(self):
        self.stub_mapper(FakeFailingRecheckMapper)

        db = db_fixture.FakeDatabase()
        db._fakedb['errors_set'] = set()
        ld = db_fixture.FakeLimitData()
        mid = middleware.TurnstileMiddleware('app', {
                'control.errors_key': 'errors_set',
                'control.errors_channel': 'errors_channel',
                })
        mid._db = db
        mid.control_daemon.get_limits = lambda: ld
        mid.limits = None  # Check that these don't get updated
        mid.mapper = None
        mid.recheck_limits()

        self.assertEqual(mid.limits, None)
        self.assertEqual(mid.limit_sum, None)
        self.assertEqual(mid.mapper, None)
        self.assertEqual(len(self.log_messages), 2)
        self.assertTrue(self.log_messages[1].startswith(
                'Could not load limits'))
        self.assertEqual(db._actions[0][0], 'sadd')
        self.assertEqual(db._actions[0][1], 'errors_set')
        self.assertTrue(db._actions[0][2].startswith(
                'Failed to load limits: '))
        self.assertEqual(db._actions[1][0], 'publish')
        self.assertEqual(db._actions[1][1], 'errors_channel')
        self.assertTrue(db._actions[1][2].startswith(
                'Failed to load limits: '))
