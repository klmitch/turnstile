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
import pkg_resources
import unittest2

from turnstile import utils


class TestException(Exception):
    pass


class TestImportClass(unittest2.TestCase):
    @mock.patch.object(pkg_resources.EntryPoint, 'parse',
                       return_value=mock.Mock(**{
                           'load.return_value': 'class',
                       }))
    def test_success(self, mock_parse):
        result = utils.import_class('test:class')

        mock_parse.assert_called_once_with('x=test:class')
        mock_parse.return_value.load.assert_called_once_with(False)

    @mock.patch.object(pkg_resources.EntryPoint, 'parse',
                       return_value=mock.Mock(**{
                           'load.side_effect': ImportError,
                       }))
    def test_with_import_error(self, mock_parse):
        self.assertRaises(ImportError, utils.import_class, 'test:class')

    @mock.patch.object(pkg_resources.EntryPoint, 'parse',
                       return_value=mock.Mock(**{
                           'load.side_effect': pkg_resources.UnknownExtra,
                       }))
    def test_with_import_error(self, mock_parse):
        self.assertRaises(ImportError, utils.import_class, 'test:class')


class TestFindEntryPoint(unittest2.TestCase):
    @mock.patch.object(pkg_resources, 'iter_entry_points',
                       return_value=[mock.Mock(**{
                           'load.return_value': 'ep1',
                       })])
    def test_straight_load(self, mock_iter_entry_points):
        result = utils.find_entrypoint('test.group', 'endpoint')

        self.assertEqual(result, 'ep1')
        mock_iter_entry_points.assert_called_once_with(
            'test.group', 'endpoint')
        mock_iter_entry_points.return_value[0].load.assert_called_once_with()

    @mock.patch.object(pkg_resources, 'iter_entry_points',
                       return_value=[
                           mock.Mock(**{
                               'load.side_effect': ImportError,
                           }),
                           mock.Mock(**{
                               'load.side_effect': pkg_resources.UnknownExtra,
                           }),
                           mock.Mock(**{
                               'load.return_value': 'ep3',
                           }),
                           mock.Mock(**{
                               'load.return_value': 'ep4',
                           }),
                       ])
    def test_skip_errors(self, mock_iter_entry_points):
        result = utils.find_entrypoint('test.group', 'endpoint')

        self.assertEqual(result, 'ep3')
        mock_iter_entry_points.assert_called_once_with(
            'test.group', 'endpoint')
        mock_iter_entry_points.return_value[0].load.assert_called_once_with()
        mock_iter_entry_points.return_value[1].load.assert_called_once_with()
        mock_iter_entry_points.return_value[2].load.assert_called_once_with()
        self.assertFalse(mock_iter_entry_points.return_value[3].load.called)

    @mock.patch.object(pkg_resources, 'iter_entry_points', return_value=[])
    def test_no_endpoints(self, mock_iter_entry_points):
        result = utils.find_entrypoint('test.group', 'endpoint')

        self.assertEqual(result, None)
        mock_iter_entry_points.assert_called_once_with(
            'test.group', 'endpoint')


class TestIgnoreExcept(unittest2.TestCase):
    def test_ignore_except(self):
        step = 0
        with utils.ignore_except():
            step += 1
            raise TestException()
            step += 2

        self.assertEqual(step, 1)
