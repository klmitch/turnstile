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

from turnstile import middleware
from turnstile import utils


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
    @mock.patch.object(utils, 'import_class')
    @mock.patch.object(utils, 'find_entrypoint')
    def test_filter_basic(self, mock_find_entrypoint, mock_import_class,
                          mock_TurnstileMiddleware):
        midware_class = middleware.turnstile_filter({})

        self.assertFalse(mock_import_class.called)
        self.assertFalse(mock_find_entrypoint.called)
        self.assertFalse(mock_TurnstileMiddleware.called)

        midware = midware_class('app')

        mock_TurnstileMiddleware.assert_called_once_with('app', {})
        self.assertEqual(midware, 'middleware')

    @mock.patch.object(middleware, 'TurnstileMiddleware')
    @mock.patch.object(utils, 'import_class',
                       return_value=mock.Mock(return_value='middleware'))
    @mock.patch.object(utils, 'find_entrypoint')
    def test_filter_alt_middleware_old(self, mock_find_entrypoint,
                                       mock_import_class,
                                       mock_TurnstileMiddleware):
        midware_class = middleware.turnstile_filter({}, turnstile='spam:ni')

        mock_import_class.assert_called_once_with('spam:ni')
        self.assertFalse(mock_import_class.return_value.called)
        self.assertFalse(mock_find_entrypoint.called)
        self.assertFalse(mock_TurnstileMiddleware.called)

        midware = midware_class('app')

        mock_import_class.return_value.assert_called_once_with(
            'app', dict(turnstile='spam:ni'))
        self.assertFalse(mock_TurnstileMiddleware.called)
        self.assertEqual(midware, 'middleware')

    @mock.patch.object(middleware, 'TurnstileMiddleware')
    @mock.patch.object(utils, 'import_class')
    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(return_value='middleware'))
    def test_filter_alt_middleware(self, mock_find_entrypoint,
                                   mock_import_class,
                                   mock_TurnstileMiddleware):
        midware_class = middleware.turnstile_filter({}, turnstile='spam')

        self.assertFalse(mock_import_class.called)
        mock_find_entrypoint.assert_called_once_with(
            'turnstile.middleware', 'spam')
        self.assertFalse(mock_find_entrypoint.return_value.called)
        self.assertFalse(mock_TurnstileMiddleware.called)

        midware = midware_class('app')

        mock_find_entrypoint.return_value.assert_called_once_with(
            'app', dict(turnstile='spam'))
        self.assertFalse(mock_TurnstileMiddleware.called)
        self.assertEqual(midware, 'middleware')

    @mock.patch.object(middleware, 'TurnstileMiddleware')
    @mock.patch.object(utils, 'import_class')
    @mock.patch.object(utils, 'find_entrypoint', return_value=None)
    def test_filter_alt_middleware_notfound(self, mock_find_entrypoint,
                                            mock_import_class,
                                            mock_TurnstileMiddleware):
        self.assertRaises(ImportError, middleware.turnstile_filter, {},
                          turnstile='spam')

        self.assertFalse(mock_import_class.called)
        mock_find_entrypoint.assert_called_once_with(
            'turnstile.middleware', 'spam')
        self.assertFalse(mock_TurnstileMiddleware.called)
