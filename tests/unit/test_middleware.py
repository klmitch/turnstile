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

import eventlet.semaphore
import mock
import unittest2

from turnstile import config
from turnstile import control
from turnstile import database
from turnstile import middleware
from turnstile import remote
from turnstile import utils


class TestException(Exception):
    pass


class TestHeadersDict(unittest2.TestCase):
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


class TestTurnstileFilter(unittest2.TestCase):
    @mock.patch.object(middleware, 'TurnstileMiddleware',
                       return_value='middleware')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_filter_basic(self, mock_find_entrypoint,
                          mock_TurnstileMiddleware):
        midware_class = middleware.turnstile_filter({})

        self.assertFalse(mock_find_entrypoint.called)
        self.assertFalse(mock_TurnstileMiddleware.called)

        midware = midware_class('app')

        mock_TurnstileMiddleware.assert_called_once_with('app', {})
        self.assertEqual(midware, 'middleware')

    @mock.patch.object(middleware, 'TurnstileMiddleware')
    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(return_value='middleware'))
    def test_filter_alt_middleware(self, mock_find_entrypoint,
                                   mock_TurnstileMiddleware):
        midware_class = middleware.turnstile_filter({}, turnstile='spam')

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.middleware', 'spam', required=True)
        self.assertFalse(mock_find_entrypoint.return_value.called)
        self.assertFalse(mock_TurnstileMiddleware.called)

        midware = midware_class('app')

        mock_find_entrypoint.return_value.assert_called_once_with(
            'app', dict(turnstile='spam'))
        self.assertFalse(mock_TurnstileMiddleware.called)
        self.assertEqual(midware, 'middleware')


class TestTurnstileMiddleware(unittest2.TestCase):
    @mock.patch.object(utils, 'find_entrypoint')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    def test_init_basic(self, mock_info, mock_RemoteControlDaemon,
                        mock_ControlDaemon, mock_find_entrypoint):
        midware = middleware.TurnstileMiddleware('app', {})

        self.assertEqual(midware.app, 'app')
        self.assertEqual(midware.limits, [])
        self.assertEqual(midware.limit_sum, None)
        self.assertEqual(midware.mapper, None)
        self.assertIsInstance(midware.mapper_lock,
                              eventlet.semaphore.Semaphore)
        self.assertEqual(midware.conf._config, {
            None: dict(status='413 Request Entity Too Large'),
        })
        self.assertEqual(midware._db, None)
        self.assertEqual(midware.preprocessors, [])
        self.assertEqual(midware.postprocessors, [])
        self.assertEqual(midware.formatter, midware.format_delay)
        self.assertFalse(mock_RemoteControlDaemon.called)
        mock_ControlDaemon.assert_has_calls([
            mock.call(midware, midware.conf),
            mock.call().start(),
        ])
        mock_info.assert_called_once_with("Turnstile middleware initialized")

    @mock.patch.object(utils, 'find_entrypoint', return_value='fake_formatter')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    def test_init_formatter(self, mock_info, mock_RemoteControlDaemon,
                            mock_ControlDaemon, mock_find_entrypoint):
        midware = middleware.TurnstileMiddleware('app',
                                                 dict(formatter='formatter'))

        self.assertEqual(midware.app, 'app')
        self.assertEqual(midware.limits, [])
        self.assertEqual(midware.limit_sum, None)
        self.assertEqual(midware.mapper, None)
        self.assertIsInstance(midware.mapper_lock,
                              eventlet.semaphore.Semaphore)
        self.assertEqual(midware.conf._config, {
            None: {
                'status': '413 Request Entity Too Large',
                'formatter': 'formatter',
            },
        })
        self.assertEqual(midware._db, None)
        self.assertEqual(midware.preprocessors, [])
        self.assertEqual(midware.postprocessors, [])
        mock_find_entrypoint.assert_called_once_with(
            'turnstile.formatter', 'formatter', required=True)
        self.assertEqual(midware.formatter, 'fake_formatter')
        self.assertFalse(mock_RemoteControlDaemon.called)
        mock_ControlDaemon.assert_has_calls([
            mock.call(midware, midware.conf),
            mock.call().start(),
        ])
        mock_info.assert_called_once_with("Turnstile middleware initialized")

    @mock.patch.object(utils, 'find_entrypoint')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    def test_init_remote(self, mock_info, mock_RemoteControlDaemon,
                         mock_ControlDaemon, mock_find_entrypoint):
        midware = middleware.TurnstileMiddleware('app', {
            'control.remote': 'yes',
        })

        self.assertEqual(midware.app, 'app')
        self.assertEqual(midware.limits, [])
        self.assertEqual(midware.limit_sum, None)
        self.assertEqual(midware.mapper, None)
        self.assertIsInstance(midware.mapper_lock,
                              eventlet.semaphore.Semaphore)
        self.assertEqual(midware.conf._config, {
            None: dict(status='413 Request Entity Too Large'),
            'control': dict(remote='yes'),
        })
        self.assertEqual(midware._db, None)
        self.assertEqual(midware.preprocessors, [])
        self.assertEqual(midware.postprocessors, [])
        self.assertEqual(midware.formatter, midware.format_delay)
        self.assertFalse(mock_ControlDaemon.called)
        mock_RemoteControlDaemon.assert_has_calls([
            mock.call(midware, midware.conf),
            mock.call().start(),
        ])
        mock_info.assert_called_once_with("Turnstile middleware initialized")

    @mock.patch.object(utils, 'find_entrypoint')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    def test_init_enable(self, mock_info, mock_RemoteControlDaemon,
                         mock_ControlDaemon, mock_find_entrypoint):
        entrypoints = {
            'turnstile.preprocessor': {
                'ep1': 'preproc1',
                'ep3': 'preproc3',
                'ep4': 'preproc4',
                'ep6': 'preproc6',
            },
            'turnstile.postprocessor': {
                'ep2': 'postproc2',
                'ep4': 'postproc4',
                'ep6': 'postproc6',
            },
        }

        mock_find_entrypoint.side_effect = \
            lambda x, y, compat=True: entrypoints[x].get(y)

        midware = middleware.TurnstileMiddleware('app', {
            'enable': 'ep1 ep2 ep3 ep4 ep5 ep6',
        })

        self.assertEqual(midware.app, 'app')
        self.assertEqual(midware.limits, [])
        self.assertEqual(midware.limit_sum, None)
        self.assertEqual(midware.mapper, None)
        self.assertIsInstance(midware.mapper_lock,
                              eventlet.semaphore.Semaphore)
        self.assertEqual(midware.conf._config, {
            None: dict(status='413 Request Entity Too Large',
                       enable='ep1 ep2 ep3 ep4 ep5 ep6'),
        })
        self.assertEqual(midware._db, None)
        self.assertEqual(midware.preprocessors, [
            'preproc1',
            'preproc3',
            'preproc4',
            'preproc6',
        ])
        self.assertEqual(midware.postprocessors, [
            'postproc6',
            'postproc4',
            'postproc2',
        ])
        self.assertEqual(midware.formatter, midware.format_delay)
        self.assertFalse(mock_RemoteControlDaemon.called)
        mock_ControlDaemon.assert_has_calls([
            mock.call(midware, midware.conf),
            mock.call().start(),
        ])
        mock_info.assert_called_once_with("Turnstile middleware initialized")

    @mock.patch.object(utils, 'find_entrypoint')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(remote, 'RemoteControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    def test_init_processors(self, mock_info, mock_RemoteControlDaemon,
                             mock_ControlDaemon, mock_find_entrypoint):
        entrypoints = {
            'turnstile.preprocessor': {
                'ep1': 'preproc1',
                'ep3': 'preproc3',
                'ep4': 'preproc4',
                'ep6': 'preproc6',
                'preproc:ep5': 'preproc5',
            },
            'turnstile.postprocessor': {
                'ep2': 'postproc2',
                'ep4': 'postproc4',
                'ep6': 'postproc6',
                'postproc:ep5': 'postproc5',
            },
        }

        mock_find_entrypoint.side_effect = \
            lambda x, y, required=False: entrypoints[x].get(y)

        midware = middleware.TurnstileMiddleware('app', {
            'preprocess': 'ep1 ep3 ep4 preproc:ep5 ep6',
            'postprocess': 'ep6 postproc:ep5 ep4 ep2',
        })

        self.assertEqual(midware.app, 'app')
        self.assertEqual(midware.limits, [])
        self.assertEqual(midware.limit_sum, None)
        self.assertEqual(midware.mapper, None)
        self.assertIsInstance(midware.mapper_lock,
                              eventlet.semaphore.Semaphore)
        self.assertEqual(midware.conf._config, {
            None: dict(status='413 Request Entity Too Large',
                       preprocess='ep1 ep3 ep4 preproc:ep5 ep6',
                       postprocess='ep6 postproc:ep5 ep4 ep2'),
        })
        self.assertEqual(midware._db, None)
        self.assertEqual(midware.preprocessors, [
            'preproc1',
            'preproc3',
            'preproc4',
            'preproc5',
            'preproc6',
        ])
        self.assertEqual(midware.postprocessors, [
            'postproc6',
            'postproc5',
            'postproc4',
            'postproc2',
        ])
        self.assertEqual(midware.formatter, midware.format_delay)
        self.assertFalse(mock_RemoteControlDaemon.called)
        mock_ControlDaemon.assert_has_calls([
            mock.call(midware, midware.conf),
            mock.call().start(),
        ])
        mock_info.assert_called_once_with("Turnstile middleware initialized")

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(),
        mock.Mock(),
    ])
    @mock.patch('routes.Mapper', return_value='mapper')
    def test_recheck_limits_basic(self, mock_Mapper, mock_limits_hydrate,
                                  mock_exception, mock_info,
                                  mock_ControlDaemon, mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.return_value': ('new_sum', ['limit1', 'limit2']),
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        midware = middleware.TurnstileMiddleware('app', {})
        midware.limits = ['old_limit1', 'old_limit2']
        midware.limit_sum = 'old_sum'
        midware.mapper = 'old_mapper'
        midware._db = mock.Mock()

        midware.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        mock_limits_hydrate.assert_called_once_with(midware._db,
                                                    ['limit1', 'limit2'])
        mock_Mapper.assert_called_once_with(register=False)
        for lim in mock_limits_hydrate.return_value:
            lim._route.assert_called_once_with('mapper')
        self.assertEqual(midware.limits, mock_limits_hydrate.return_value)
        self.assertEqual(midware.limit_sum, 'new_sum')
        self.assertEqual(midware.mapper, 'mapper')
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)
        self.assertEqual(len(midware._db.method_calls), 0)

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(),
        mock.Mock(),
    ])
    @mock.patch('routes.Mapper', return_value='mapper')
    def test_recheck_limits_unchanged(self, mock_Mapper, mock_limits_hydrate,
                                      mock_exception, mock_info,
                                      mock_ControlDaemon, mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': control.NoChangeException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        midware = middleware.TurnstileMiddleware('app', {})
        midware.limits = ['old_limit1', 'old_limit2']
        midware.limit_sum = 'old_sum'
        midware.mapper = 'old_mapper'
        midware._db = mock.Mock()

        midware.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertFalse(mock_Mapper.called)
        for lim in mock_limits_hydrate.return_value:
            self.assertFalse(lim._route.called)
        self.assertEqual(midware.limits, ['old_limit1', 'old_limit2'])
        self.assertEqual(midware.limit_sum, 'old_sum')
        self.assertEqual(midware.mapper, 'old_mapper')
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_format_exc.called)
        self.assertEqual(len(midware._db.method_calls), 0)

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(),
        mock.Mock(),
    ])
    @mock.patch('routes.Mapper', return_value='mapper')
    def test_recheck_limits_exception(self, mock_Mapper, mock_limits_hydrate,
                                      mock_exception, mock_info,
                                      mock_ControlDaemon, mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': TestException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        midware = middleware.TurnstileMiddleware('app', {})
        midware.limits = ['old_limit1', 'old_limit2']
        midware.limit_sum = 'old_sum'
        midware.mapper = 'old_mapper'
        midware._db = mock.Mock()

        midware.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertFalse(mock_Mapper.called)
        for lim in mock_limits_hydrate.return_value:
            self.assertFalse(lim._route.called)
        self.assertEqual(midware.limits, ['old_limit1', 'old_limit2'])
        self.assertEqual(midware.limit_sum, 'old_sum')
        self.assertEqual(midware.mapper, 'old_mapper')
        mock_exception.assert_called_once_with("Could not load limits")
        mock_format_exc.assert_called_once_with()
        midware._db.assert_has_calls([
            mock.call.sadd('errors', 'Failed to load limits: <traceback>'),
            mock.call.publish('errors', 'Failed to load limits: <traceback>'),
        ])

    @mock.patch('traceback.format_exc', return_value='<traceback>')
    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.LOG, 'exception')
    @mock.patch.object(database, 'limits_hydrate', return_value=[
        mock.Mock(),
        mock.Mock(),
    ])
    @mock.patch('routes.Mapper', return_value='mapper')
    def test_recheck_limits_exception_altkeys(self, mock_Mapper,
                                              mock_limits_hydrate,
                                              mock_exception, mock_info,
                                              mock_ControlDaemon,
                                              mock_format_exc):
        limit_data = mock.Mock(**{
            'get_limits.side_effect': TestException,
        })
        mock_ControlDaemon.return_value = mock.Mock(**{
            'get_limits.return_value': limit_data,
        })
        midware = middleware.TurnstileMiddleware('app', {
            'control.errors_key': 'eset',
            'control.errors_channel': 'epub',
        })
        midware.limits = ['old_limit1', 'old_limit2']
        midware.limit_sum = 'old_sum'
        midware.mapper = 'old_mapper'
        midware._db = mock.Mock()

        midware.recheck_limits()

        mock_ControlDaemon.return_value.get_limits.assert_called_once_with()
        limit_data.get_limits.assert_called_once_with('old_sum')
        self.assertFalse(mock_limits_hydrate.called)
        self.assertFalse(mock_Mapper.called)
        for lim in mock_limits_hydrate.return_value:
            self.assertFalse(lim._route.called)
        self.assertEqual(midware.limits, ['old_limit1', 'old_limit2'])
        self.assertEqual(midware.limit_sum, 'old_sum')
        self.assertEqual(midware.mapper, 'old_mapper')
        mock_exception.assert_called_once_with("Could not load limits")
        mock_format_exc.assert_called_once_with()
        midware._db.assert_has_calls([
            mock.call.sadd('eset', 'Failed to load limits: <traceback>'),
            mock.call.publish('epub', 'Failed to load limits: <traceback>'),
        ])

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.TurnstileMiddleware, 'recheck_limits')
    @mock.patch.object(middleware.TurnstileMiddleware, 'format_delay',
                       return_value='formatted delay')
    def test_call_basic(self, mock_format_delay, mock_recheck_limits,
                        mock_info, mock_ControlDaemon):
        app = mock.Mock(return_value='app response')
        midware = middleware.TurnstileMiddleware(app, {})
        midware.mapper = mock.Mock()
        environ = {}

        result = midware(environ, 'start_response')

        self.assertEqual(result, 'app response')
        mock_recheck_limits.assert_called_once_with()
        midware.mapper.routematch.assert_called_once_with(environ=environ)
        self.assertFalse(mock_format_delay.called)
        app.assert_called_once_with(environ, 'start_response')
        self.assertEqual(environ, {
            'turnstile.conf': midware.conf,
        })

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.TurnstileMiddleware, 'recheck_limits')
    @mock.patch.object(middleware.TurnstileMiddleware, 'format_delay',
                       return_value='formatted delay')
    def test_call_processors(self, mock_format_delay, mock_recheck_limits,
                             mock_info, mock_ControlDaemon):
        app = mock.Mock(return_value='app response')
        midware = middleware.TurnstileMiddleware(app, {})
        midware.mapper = mock.Mock()
        midware.preprocessors = [mock.Mock(), mock.Mock()]
        midware.postprocessors = [mock.Mock(), mock.Mock()]
        environ = {}

        result = midware(environ, 'start_response')

        self.assertEqual(result, 'app response')
        mock_recheck_limits.assert_called_once_with()
        for proc in midware.preprocessors:
            proc.assert_called_once_with(midware, environ)
        midware.mapper.routematch.assert_called_once_with(environ=environ)
        self.assertFalse(mock_format_delay.called)
        for proc in midware.postprocessors:
            proc.assert_called_once_with(midware, environ)
        app.assert_called_once_with(environ, 'start_response')
        self.assertEqual(environ, {
            'turnstile.conf': midware.conf,
        })

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware.TurnstileMiddleware, 'recheck_limits')
    @mock.patch.object(middleware.TurnstileMiddleware, 'format_delay',
                       return_value='formatted delay')
    def test_call_delay(self, mock_format_delay, mock_recheck_limits,
                        mock_info, mock_ControlDaemon):
        app = mock.Mock(return_value='app response')
        midware = middleware.TurnstileMiddleware(app, {})
        midware.mapper = mock.Mock()
        midware.preprocessors = [mock.Mock(), mock.Mock()]
        midware.postprocessors = [mock.Mock(), mock.Mock()]
        environ = {
            'turnstile.delay': [
                (30, 'limit1', 'bucket1'),
                (20, 'limit2', 'bucket2'),
                (60, 'limit3', 'bucket3'),
                (10, 'limit4', 'bucket4'),
            ],
        }

        result = midware(environ, 'start_response')

        self.assertEqual(result, 'formatted delay')
        mock_recheck_limits.assert_called_once_with()
        for proc in midware.preprocessors:
            proc.assert_called_once_with(midware, environ)
        midware.mapper.routematch.assert_called_once_with(environ=environ)
        mock_format_delay.assert_called_once_with(60, 'limit3', 'bucket3',
                                                  environ, 'start_response')
        for proc in midware.postprocessors:
            self.assertFalse(proc.called)
        self.assertFalse(app.called)
        self.assertEqual(environ, {
            'turnstile.delay': [
                (30, 'limit1', 'bucket1'),
                (20, 'limit2', 'bucket2'),
                (60, 'limit3', 'bucket3'),
                (10, 'limit4', 'bucket4'),
            ],
            'turnstile.conf': midware.conf,
        })

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware, 'HeadersDict', return_value=mock.Mock(**{
        'items.return_value': 'header items',
    }))
    def test_format_delay(self, mock_HeadersDict, mock_info,
                          mock_ControlDaemon):
        midware = middleware.TurnstileMiddleware('app', {})
        limit = mock.Mock(**{
            'format.return_value': ('limit status', 'limit entity'),
        })
        start_response = mock.Mock()

        result = midware.format_delay(10.1, limit, 'bucket', 'environ',
                                      start_response)

        self.assertEqual(result, 'limit entity')
        mock_HeadersDict.assert_called_once_with([('Retry-After', '11')])
        limit.format.assert_called_once_with(
            '413 Request Entity Too Large', mock_HeadersDict.return_value,
            'environ', 'bucket', 10.1)
        start_response.assert_called_once_with(
            'limit status', 'header items')

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(middleware, 'HeadersDict', return_value=mock.Mock(**{
        'items.return_value': 'header items',
    }))
    def test_format_delay_altstatus(self, mock_HeadersDict, mock_info,
                                    mock_ControlDaemon):
        midware = middleware.TurnstileMiddleware('app', {
            'status': 'some other status',
        })
        limit = mock.Mock(**{
            'format.return_value': ('limit status', 'limit entity'),
        })
        start_response = mock.Mock()

        result = midware.format_delay(10.1, limit, 'bucket', 'environ',
                                      start_response)

        self.assertEqual(result, 'limit entity')
        mock_HeadersDict.assert_called_once_with([('Retry-After', '11')])
        limit.format.assert_called_once_with(
            'some other status', mock_HeadersDict.return_value,
            'environ', 'bucket', 10.1)
        start_response.assert_called_once_with(
            'limit status', 'header items')

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(config.Config, 'get_database', return_value='database')
    def test_db(self, mock_get_database, mock_info, mock_ControlDaemon):
        midware = middleware.TurnstileMiddleware('app', {})

        db = midware.db

        self.assertEqual(db, 'database')
        mock_get_database.assert_called_once_with()

    @mock.patch.object(control, 'ControlDaemon')
    @mock.patch.object(middleware.LOG, 'info')
    @mock.patch.object(config.Config, 'get_database', return_value='database')
    def test_db_cached(self, mock_get_database, mock_info, mock_ControlDaemon):
        midware = middleware.TurnstileMiddleware('app', {})
        midware._db = 'cached'

        db = midware.db

        self.assertEqual(db, 'cached')
        self.assertFalse(mock_get_database.called)
