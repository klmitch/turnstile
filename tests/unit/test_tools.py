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

import argparse
import inspect
import StringIO
import sys

from lxml import etree
import mock
import unittest2

from turnstile import compactor
from turnstile import config
from turnstile import database
from turnstile import limits
from turnstile import remote
from turnstile import tools
from turnstile import utils

from tests.unit import utils as test_utils


limit_attrs = {
    'bool_attr': {
        'type': bool,
        'default': False,
    },
    'int_attr': {
        'type': int,
        'default': 1,
    },
    'float_attr': {
        'type': float,
        'default': 1.5,
    },
    'str_attr': {
        'type': str,
        'default': 'spam',
    },
    'bad_attr': {
        'type': mock.Mock(side_effect=ValueError),
        'default': None,
    },
    'list_attr': {
        'type': list,
        'subtype': int,
        'default': lambda: [],
    },
    'dict_attr': {
        'type': dict,
        'subtype': int,
        'default': lambda: {},
    },
    'required': {
        'type': str,
    },
}

limit_values = {
    'bool_attr': False,
    'int_attr': 1,
    'float_attr': 1.5,
    'str_attr': 'spam',
    'bad_attr': None,
    'list_attr': [],
    'dict_attr': {},
    'required': 'required',
}


class TestScriptAdaptor(unittest2.TestCase):
    def test_init(self):
        func = mock.Mock(__doc__="\n    this is\n    a test  \n\n    of this")
        sa = tools.ScriptAdaptor(func)

        self.assertEqual(sa._func, func)
        self.assertEqual(sa._preprocess, [])
        self.assertEqual(sa._postprocess, [])
        self.assertEqual(sa._arguments, [])
        self.assertEqual(sa.description, 'this is a test')

    @mock.patch('functools.update_wrapper', side_effect=lambda x, y: x)
    def test_wrap_unwrapped(self, mock_update_wrapper):
        func = mock.Mock(__doc__="\n    this is\n    a test  \n\n    of this")

        sa = tools.ScriptAdaptor._wrap(func)

        self.assertIsInstance(sa, tools.ScriptAdaptor)
        self.assertEqual(sa._func, func)
        self.assertEqual(sa._preprocess, [])
        self.assertEqual(sa._postprocess, [])
        self.assertEqual(sa._arguments, [])
        self.assertEqual(sa.description, 'this is a test')
        mock_update_wrapper.assert_called_once_with(sa, func)

    @mock.patch('functools.update_wrapper', side_effect=lambda x, y: x)
    def test_wrap_wrapped(self, mock_update_wrapper):
        func = mock.Mock(__doc__="\n    this is\n    a test  \n\n    of this")
        sa = tools.ScriptAdaptor(func)

        result = tools.ScriptAdaptor._wrap(sa)

        self.assertEqual(sa, result)
        self.assertEqual(sa._func, func)
        self.assertEqual(sa._preprocess, [])
        self.assertEqual(sa._postprocess, [])
        self.assertEqual(sa._arguments, [])
        self.assertEqual(sa.description, 'this is a test')
        self.assertFalse(mock_update_wrapper.called)

    def test_call(self):
        func = mock.Mock(__doc__='', return_value="result")
        sa = tools.ScriptAdaptor(func)

        result = sa(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, 'result')
        func.assert_called_once_with(1, 2, 3, a=4, b=5, c=6)

    def test_add_argument(self):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)

        sa._add_argument((1, 2, 3), dict(a=4, b=5, c=6))
        sa._add_argument((3, 2, 1), dict(c=4, b=5, a=6))

        self.assertEqual(sa._arguments, [
            ((3, 2, 1), dict(c=4, b=5, a=6)),
            ((1, 2, 3), dict(a=4, b=5, c=6)),
        ])

    def test_add_preprocessor(self):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)

        sa._add_preprocessor('preproc1')
        sa._add_preprocessor('preproc2')

        self.assertEqual(sa._preprocess, [
            'preproc2',
            'preproc1',
        ])

    def test_add_postprocessor(self):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)

        sa._add_postprocessor('postproc1')
        sa._add_postprocessor('postproc2')

        self.assertEqual(sa._postprocess, [
            'postproc2',
            'postproc1',
        ])

    def test_setup_args(self):
        parser = mock.Mock()
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        sa._arguments = [
            ((1, 2, 3), dict(a=4, b=5, c=6)),
            ((3, 2, 1), dict(c=4, b=5, a=6)),
        ]

        sa.setup_args(parser)

        parser.assert_has_calls([
            mock.call.add_argument(1, 2, 3, a=4, b=5, c=6),
            mock.call.add_argument(3, 2, 1, c=4, b=5, a=6),
        ])

    @mock.patch.object(inspect, 'getargspec', return_value=inspect.ArgSpec(
        ('a', 'b', 'c'), None, None, None))
    def test_get_kwargs(self, mock_getargspec):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(a=1, b=2, c=3, d=4)

        result = sa.get_kwargs(args)

        self.assertEqual(result, dict(a=1, b=2, c=3))
        mock_getargspec.assert_called_once_with(func)

    @mock.patch.object(inspect, 'getargspec', return_value=inspect.ArgSpec(
        ('a', 'b', 'c'), None, 'kwargs', None))
    def test_get_kwargs_with_kwargs(self, mock_getargspec):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(a=1, b=2, c=3, d=4)

        result = sa.get_kwargs(args)

        self.assertEqual(result, dict(a=1, b=2, c=3, d=4))
        mock_getargspec.assert_called_once_with(func)

    @mock.patch.object(inspect, 'getargspec', return_value=inspect.ArgSpec(
        ('a', 'b', 'c'), None, None, None))
    def test_get_kwargs_missing_required(self, mock_getargspec):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(a=1, b=2)

        self.assertRaises(AttributeError, sa.get_kwargs, args)
        mock_getargspec.assert_called_once_with(func)

    @mock.patch.object(inspect, 'getargspec', return_value=inspect.ArgSpec(
        ('a', 'b', 'c'), None, None, (10,)))
    def test_get_kwargs_missing_required_withdefs(self, mock_getargspec):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(a=1)

        self.assertRaises(AttributeError, sa.get_kwargs, args)
        mock_getargspec.assert_called_once_with(func)

    @mock.patch.object(inspect, 'getargspec', return_value=inspect.ArgSpec(
        ('a', 'b', 'c'), None, None, (10,)))
    def test_get_kwargs_missing_optionals(self, mock_getargspec):
        func = mock.Mock(__doc__='')
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(a=1, b=2)

        result = sa.get_kwargs(args)

        self.assertEqual(result, dict(a=1, b=2))
        mock_getargspec.assert_called_once_with(func)

    def test_safe_call(self):
        func = mock.Mock(__doc__='', return_value='result')
        sa = tools.ScriptAdaptor(func)

        result = sa.safe_call(dict(a=1, b=2, c=3))

        self.assertEqual(result, 'result')
        func.assert_called_once_with(a=1, b=2, c=3)

    def test_safe_call_error_noargs(self):
        func = mock.Mock(__doc__='',
                         side_effect=test_utils.TestException("error"))
        sa = tools.ScriptAdaptor(func)

        result = sa.safe_call(dict(a=1, b=2, c=3))

        self.assertEqual(result, 'error')
        func.assert_called_once_with(a=1, b=2, c=3)

    def test_safe_call_error_debug(self):
        func = mock.Mock(__doc__='',
                         side_effect=test_utils.TestException("error"))
        sa = tools.ScriptAdaptor(func)
        args = argparse.Namespace(debug=True)

        self.assertRaises(test_utils.TestException, sa.safe_call,
                          dict(a=1, b=2, c=3), args)
        func.assert_called_once_with(a=1, b=2, c=3)

    @mock.patch.object(argparse, 'ArgumentParser', return_value=mock.Mock(**{
        'parse_args.return_value': 'parsed args',
    }))
    @mock.patch.object(tools.ScriptAdaptor, 'setup_args')
    @mock.patch.object(tools.ScriptAdaptor, 'safe_call', return_value='result')
    @mock.patch.object(tools.ScriptAdaptor, 'get_kwargs',
                       return_value=dict(a=1, b=2, c=3))
    def test_console(self, mock_get_kwargs, mock_safe_call, mock_setup_args,
                     mock_ArgumentParser):
        func = mock.Mock(__doc__='\n    this is\n     a test\n\n    of this')
        sa = tools.ScriptAdaptor(func)
        sa._preprocess = [mock.Mock(), mock.Mock()]
        sa._postprocess = [mock.Mock(return_value='postprocessed')]

        result = sa.console()

        self.assertEqual(result, 'postprocessed')
        mock_ArgumentParser.assert_called_once_with(
            description='this is a test')
        mock_setup_args.assert_called_once_with(
            mock_ArgumentParser.return_value)
        mock_ArgumentParser.return_value.parse_args.assert_called_once_with()
        sa._preprocess[0].assert_called_once_with('parsed args')
        sa._preprocess[1].assert_called_once_with('parsed args')
        mock_get_kwargs.assert_called_once_with('parsed args')
        mock_safe_call.assert_called_once_with(
            dict(a=1, b=2, c=3), 'parsed args')
        sa._postprocess[0].assert_called_once_with('parsed args', 'result')

    @mock.patch.object(argparse, 'ArgumentParser', return_value=mock.Mock(**{
        'parse_args.return_value': 'parsed args',
    }))
    @mock.patch.object(tools.ScriptAdaptor, 'setup_args')
    @mock.patch.object(tools.ScriptAdaptor, 'safe_call', return_value='result')
    @mock.patch.object(tools.ScriptAdaptor, 'get_kwargs',
                       return_value=dict(a=1, b=2, c=3))
    def test_console_failed_preproc(self, mock_get_kwargs, mock_safe_call,
                                    mock_setup_args, mock_ArgumentParser):
        func = mock.Mock(__doc__='\n    this is\n     a test\n\n    of this')
        sa = tools.ScriptAdaptor(func)
        sa._preprocess = [
            mock.Mock(side_effect=test_utils.TestException("badness")),
        ]

        result = sa.console()

        self.assertEqual(result, 'badness')
        mock_ArgumentParser.assert_called_once_with(
            description='this is a test')
        mock_setup_args.assert_called_once_with(
            mock_ArgumentParser.return_value)
        mock_ArgumentParser.return_value.parse_args.assert_called_once_with()
        sa._preprocess[0].assert_called_once_with('parsed args')
        self.assertFalse(mock_get_kwargs.called)
        self.assertFalse(mock_safe_call.called)

    @mock.patch.object(argparse, 'ArgumentParser', return_value=mock.Mock(**{
        'parse_args.return_value': mock.Mock(debug=True),
    }))
    @mock.patch.object(tools.ScriptAdaptor, 'setup_args')
    @mock.patch.object(tools.ScriptAdaptor, 'safe_call', return_value='result')
    @mock.patch.object(tools.ScriptAdaptor, 'get_kwargs',
                       return_value=dict(a=1, b=2, c=3))
    def test_console_failed_preproc_debug(self, mock_get_kwargs,
                                          mock_safe_call, mock_setup_args,
                                          mock_ArgumentParser):
        func = mock.Mock(__doc__='\n    this is\n     a test\n\n    of this')
        sa = tools.ScriptAdaptor(func)
        sa._preprocess = [
            mock.Mock(side_effect=test_utils.TestException("badness")),
        ]

        self.assertRaises(test_utils.TestException, sa.console)

        mock_ArgumentParser.assert_called_once_with(
            description='this is a test')
        mock_setup_args.assert_called_once_with(
            mock_ArgumentParser.return_value)
        mock_ArgumentParser.return_value.parse_args.assert_called_once_with()
        sa._preprocess[0].assert_called_once_with(
            mock_ArgumentParser.return_value.parse_args.return_value)
        self.assertFalse(mock_get_kwargs.called)
        self.assertFalse(mock_safe_call.called)


class TestAddArgument(unittest2.TestCase):
    @mock.patch.object(tools.ScriptAdaptor, '_wrap', return_value=mock.Mock())
    def test_decorator(self, mock_wrap):
        decorator = tools.add_argument(1, 2, 3, a=4, b=5, c=6)

        self.assertTrue(callable(decorator))
        self.assertFalse(mock_wrap.called)

        result = decorator('func')

        self.assertEqual(result, mock_wrap.return_value)
        mock_wrap.assert_called_once_with('func')
        mock_wrap.return_value._add_argument.assert_called_once_with(
            (1, 2, 3), dict(a=4, b=5, c=6))


class TestAddPreprocessor(unittest2.TestCase):
    @mock.patch.object(tools.ScriptAdaptor, '_wrap', return_value=mock.Mock())
    def test_decorator(self, mock_wrap):
        decorator = tools.add_preprocessor('preproc')

        self.assertTrue(callable(decorator))
        self.assertFalse(mock_wrap.called)

        result = decorator('func')

        self.assertEqual(result, mock_wrap.return_value)
        mock_wrap.assert_called_once_with('func')
        mock_wrap.return_value._add_preprocessor.assert_called_once_with(
            'preproc')


class TestAddPostprocessor(unittest2.TestCase):
    @mock.patch.object(tools.ScriptAdaptor, '_wrap', return_value=mock.Mock())
    def test_decorator(self, mock_wrap):
        decorator = tools.add_postprocessor('postproc')

        self.assertTrue(callable(decorator))
        self.assertFalse(mock_wrap.called)

        result = decorator('func')

        self.assertEqual(result, mock_wrap.return_value)
        mock_wrap.assert_called_once_with('func')
        mock_wrap.return_value._add_postprocessor.assert_called_once_with(
            'postproc')


class TestSetupLogging(unittest2.TestCase):
    @mock.patch('logging.config.fileConfig')
    @mock.patch('logging.basicConfig')
    def test_basic(self, mock_basicConfig, mock_fileConfig):
        tools._setup_logging(argparse.Namespace())

        self.assertFalse(mock_fileConfig.called)
        mock_basicConfig.assert_called_once_with()

    @mock.patch('logging.config.fileConfig')
    @mock.patch('logging.basicConfig')
    def test_file(self, mock_basicConfig, mock_fileConfig):
        tools._setup_logging(argparse.Namespace(logging='log.conf'))

        mock_fileConfig.assert_called_once_with('log.conf')
        self.assertFalse(mock_basicConfig.called)


class TestParseLimitNode(unittest2.TestCase):
    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_basic(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_missing_requirement(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
</limit>
""")

        self.assertRaises(TypeError, tools.parse_limit_node,
                          'db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        self.assertFalse(limit_class.called)

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_unknown_elem(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attribute name="badelem">spam</attribute>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Unrecognized element 'attribute' while parsing limit at "
            "index 1; ignoring...")
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_unknown_attr(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="unknown_attr">spam</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Limit at index 1 does not accept an attribute "
            "'unknown_attr'; ignoring...")
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_bool_attr_true(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="bool_attr">True</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam',
                                            bool_attr=True)

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_bool_attr_false(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="bool_attr">False</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam',
                                            bool_attr=False)

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_bool_attr_unknown(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="bool_attr">unknown</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Unrecognized boolean value 'unknown' while parsing "
            "'bool_attr' attribute of limit at index 1; ignoring...")
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_basic_atrs(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="int_attr">5</attr>
    <attr name="float_attr">30.1</attr>
    <attr name="str_attr">ni</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam',
                                            int_attr=5,
                                            float_attr=30.1,
                                            str_attr='ni')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_invalid_basic_attr(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="bad_attr">ni</attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Invalid value 'ni' while parsing 'bad_attr' attribute of limit "
            "at index 1; ignoring...")
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_list_attr(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="list_attr">
        <value>3</value>
        <value>5</value>
        <value>7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam',
                                            list_attr=[3, 5, 7])

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_list_attr_unknown_elem(self, mock_warn,
                                          mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="list_attr">
        <val>3</val>
        <value>5</value>
        <value>7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Unrecognized element 'val' while parsing 'list_attr' attribute "
            "of limit at index 1; ignoring element...")
        limit_class.assert_called_once_with('db', required='spam',
                                            list_attr=[5, 7])

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_list_attr_bad_value(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="list_attr">
        <value>3</value>
        <value>5</value>
        <value>spam</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Invalid value 'spam' while parsing element 2 of 'list_attr' "
            "attribute of limit at index 1; ignoring attribute...")
        limit_class.assert_called_once_with('db', required='spam')

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_dict_attr(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="dict_attr">
        <value key="three">3</value>
        <value key="five">5</value>
        <value key="seven">7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        self.assertFalse(mock_warn.called)
        limit_class.assert_called_once_with('db', required='spam',
                                            dict_attr=dict(three=3, five=5,
                                                           seven=7))

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_dict_attr_unknown_elem(self, mock_warn,
                                          mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="dict_attr">
        <val key="three">3</val>
        <value key="five">5</value>
        <value key="seven">7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Unrecognized element 'val' while parsing 'dict_attr' attribute "
            "of limit at index 1; ignoring element...")
        limit_class.assert_called_once_with('db', required='spam',
                                            dict_attr=dict(five=5, seven=7))

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_dict_attr_missing_key(self, mock_warn,
                                         mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="dict_attr">
        <value badkey="three">3</value>
        <value key="five">5</value>
        <value key="seven">7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Missing 'key' attribute of 'value' element while parsing "
            "'dict_attr' attribute of limit at index 1; ignoring element...")
        limit_class.assert_called_once_with('db', required='spam',
                                            dict_attr=dict(five=5, seven=7))

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=mock.Mock(attrs=limit_attrs))
    @mock.patch('warnings.warn')
    def test_parse_dict_attr_bad_value(self, mock_warn, mock_find_entrypoint):
        limit_class = mock_find_entrypoint.return_value
        limit_node = etree.fromstring("""
<limit class="FakeLimit">
    <attr name="dict_attr">
        <value key="three">three</value>
        <value key="five">5</value>
        <value key="seven">7</value>
    </attr>
    <attr name="required">spam</attr>
</limit>
""")

        limit = tools.parse_limit_node('db', 1, limit_node)

        mock_find_entrypoint.assert_called_once_with(
            'turnstile.limit', 'FakeLimit', required=True)
        mock_warn.assert_called_once_with(
            "Invalid value 'three' while parsing 'three' element of "
            "'dict_attr' attribute of limit at index 1; ignoring element...")
        limit_class.assert_called_once_with('db', required='spam',
                                            dict_attr=dict(five=5, seven=7))


class TestMakeLimitNode(unittest2.TestCase):
    def _make_limit(self, **kwargs):
        limit_kwargs = limit_values.copy()
        limit_kwargs.update(kwargs)
        return mock.Mock(_limit_full_name='FakeLimit', attrs=limit_attrs,
                         **limit_kwargs)

    def test_make_basic(self):
        root = etree.Element('root')
        limit = self._make_limit()

        tools.make_limit_node(root, limit)

        test_utils.compare_xml(actual=root, expected="""
<root>
    <limit class="FakeLimit">
        <attr name="required">required</attr>
    </limit>
</root>
""")

    def test_make_basic_attrs(self):
        root = etree.Element('root')
        limit = self._make_limit(bool_attr=True, int_attr=5, float_attr=30.1,
                                 str_attr="something")

        tools.make_limit_node(root, limit)

        test_utils.compare_xml(actual=root, expected="""
<root>
    <limit class="FakeLimit">
        <attr name="bool_attr">True</attr>
        <attr name="float_attr">30.1</attr>
        <attr name="int_attr">5</attr>
        <attr name="required">required</attr>
        <attr name="str_attr">something</attr>
    </limit>
</root>
""")

    def test_make_list_attrs(self):
        root = etree.Element('root')
        limit = self._make_limit(list_attr=[3, 5, 7])

        tools.make_limit_node(root, limit)

        test_utils.compare_xml(actual=root, expected="""
<root>
    <limit class="FakeLimit">
        <attr name="list_attr">
            <value>3</value>
            <value>5</value>
            <value>7</value>
        </attr>
        <attr name="required">required</attr>
    </limit>
</root>
""")

    def test_make_dict_attrs(self):
        root = etree.Element('root')
        limit = self._make_limit(dict_attr=dict(three=3, five=5, seven=7))

        tools.make_limit_node(root, limit)

        test_utils.compare_xml(actual=root, expected="""
<root>
    <limit class="FakeLimit">
        <attr name="dict_attr">
            <value key="five">5</value>
            <value key="seven">7</value>
            <value key="three">3</value>
        </attr>
        <attr name="required">required</attr>
    </limit>
</root>
""")


class TestConsoleScripts(unittest2.TestCase):
    def test_setup_limits(self):
        self.assertIsInstance(tools.setup_limits, tools.ScriptAdaptor)
        self.assertGreater(len(tools.setup_limits._arguments), 0)

    def test_dump_limits(self):
        self.assertIsInstance(tools.dump_limits, tools.ScriptAdaptor)
        self.assertGreater(len(tools.dump_limits._arguments), 0)

    def test_remote_daemon(self):
        self.assertIsInstance(tools.remote_daemon, tools.ScriptAdaptor)
        self.assertGreater(len(tools.remote_daemon._arguments), 0)

    def test_turnstile_command(self):
        self.assertIsInstance(tools.turnstile_command, tools.ScriptAdaptor)
        self.assertGreater(len(tools.turnstile_command._arguments), 0)

    def test_compactor_daemon(self):
        self.assertIsInstance(tools.compactor_daemon, tools.ScriptAdaptor)
        self.assertGreater(len(tools.compactor_daemon._arguments), 0)


class TestSetupLimits(unittest2.TestCase):
    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_basic(self, mock_parse_limit_node, mock_limit_update,
                   mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(6)])
        mock_command.assert_called_once_with('db', 'control', 'reload')
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_altconf(self, mock_parse_limit_node, mock_limit_update,
                     mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = dict(
            limits_key='alt_lims',
            channel='alt_chan',
        )

        tools.setup_limits('conf_file', 'limits_file')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'alt_lims', ['limit%d:%d' % (i, i) for i in range(6)])
        mock_command.assert_called_once_with('db', 'alt_chan', 'reload')
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='badtag', idx=0),
            mock.Mock(tag='limit', idx=1),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=test_utils.TestException("spam"))
    def test_warnings(self, mock_parse_limit_node, mock_limit_update,
                      mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        mock_warn.assert_has_calls([
            mock.call("Unrecognized tag 'badtag' in limits file at index 0"),
            mock.call("Couldn't understand limit at index 1: spam"),
        ])
        mock_limit_update.assert_called_once_with('db', 'limits', [])
        mock_command.assert_called_once_with('db', 'control', 'reload')
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.MagicMock(tag='limit', idx=0),
            mock.MagicMock(tag='limit', idx=1),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_debug(self, mock_parse_limit_node, mock_limit_update,
                   mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', debug=True)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(2)])
        mock_command.assert_called_once_with('db', 'control', 'reload')
        self.assertEqual(sys.stderr.getvalue(),
                         'Installing the following limits:\n'
                         "  'limit0:0'\n"
                         "  'limit1:1'\n"
                         'Issuing command: reload\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.MagicMock(tag='limit', idx=0),
            mock.MagicMock(tag='limit', idx=1),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_dryrun(self, mock_parse_limit_node, mock_limit_update,
                    mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', dry_run=True)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
        ])
        self.assertFalse(mock_limit_update.called)
        self.assertFalse(mock_command.called)
        self.assertEqual(sys.stderr.getvalue(),
                         'Installing the following limits:\n'
                         "  'limit0:0'\n"
                         "  'limit1:1'\n"
                         'Issuing command: reload\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_noreload(self, mock_parse_limit_node, mock_limit_update,
                      mock_command, mock_Config, mock_warn, mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', do_reload=False)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(6)])
        self.assertFalse(mock_command.called)
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_reload_spread_int(self, mock_parse_limit_node, mock_limit_update,
                               mock_command, mock_Config, mock_warn,
                               mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', do_reload=42)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(6)])
        mock_command.assert_called_once_with(
            'db', 'control', 'reload', 'spread', 42)
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_reload_spread_str(self, mock_parse_limit_node, mock_limit_update,
                               mock_command, mock_Config, mock_warn,
                               mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', do_reload='42')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(6)])
        mock_command.assert_called_once_with(
            'db', 'control', 'reload', 'spread', '42')
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.parse', return_value=mock.Mock(**{
        'getroot.return_value': [
            mock.Mock(tag='limit', idx=0),
            mock.Mock(tag='limit', idx=1),
            mock.Mock(tag='limit', idx=2),
            mock.Mock(tag='limit', idx=3),
            mock.Mock(tag='limit', idx=4),
            mock.Mock(tag='limit', idx=5),
        ]
    }))
    @mock.patch('warnings.warn')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': 'db',
    }))
    @mock.patch.object(database, 'command')
    @mock.patch.object(database, 'limit_update')
    @mock.patch.object(tools, 'parse_limit_node',
                       side_effect=lambda x, y, z: 'limit%d:%d' % (y, z.idx))
    def test_reload_spread_str(self, mock_parse_limit_node, mock_limit_update,
                               mock_command, mock_Config, mock_warn,
                               mock_etree_parse):
        limits_tree = mock_etree_parse.return_value
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}

        tools.setup_limits('conf_file', 'limits_file', do_reload='immediate')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_etree_parse.assert_called_once_with('limits_file')
        limits_tree.getroot.assert_called_once_with()
        self.assertFalse(mock_warn.called)
        mock_parse_limit_node.assert_has_calls([
            mock.call('db', 0, limits_tree.getroot.return_value[0]),
            mock.call('db', 1, limits_tree.getroot.return_value[1]),
            mock.call('db', 2, limits_tree.getroot.return_value[2]),
            mock.call('db', 3, limits_tree.getroot.return_value[3]),
            mock.call('db', 4, limits_tree.getroot.return_value[4]),
            mock.call('db', 5, limits_tree.getroot.return_value[5]),
        ])
        mock_limit_update.assert_called_once_with(
            'db', 'limits', ['limit%d:%d' % (i, i) for i in range(6)])
        mock_command.assert_called_once_with(
            'db', 'control', 'reload', 'immediate')
        self.assertEqual(sys.stderr.getvalue(), '')


class TestDumpLimits(unittest2.TestCase):
    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.Element', return_value=mock.Mock())
    @mock.patch('lxml.etree.ElementTree', return_value=mock.Mock())
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'zrange.return_value': [
                'limit0',
                'limit1',
                'limit2',
            ],
        })}))
    @mock.patch.object(limits.Limit, 'hydrate', side_effect=lambda x, y: y)
    @mock.patch.object(tools, 'make_limit_node')
    def test_basic(self, mock_make_limit_node, mock_hydrate, mock_Config,
                   mock_loads, mock_ElementTree, mock_Element):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        root = mock_Element.return_value
        limit_tree = mock_ElementTree.return_value

        tools.dump_limits('conf_file', 'limits_file')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        db.zrange.assert_called_once_with('limits', 0, -1)
        mock_loads.assert_has_calls([
            mock.call('limit0'),
            mock.call('limit1'),
            mock.call('limit2'),
        ])
        mock_hydrate.assert_has_calls([
            mock.call(db, 'limit0'),
            mock.call(db, 'limit1'),
            mock.call(db, 'limit2'),
        ])
        mock_Element.assert_called_once_with('limits')
        mock_ElementTree.assert_called_once_with(root)
        mock_make_limit_node.assert_has_calls([
            mock.call(root, 'limit0'),
            mock.call(root, 'limit1'),
            mock.call(root, 'limit2'),
        ])
        limit_tree.write.assert_called_once_with(
            'limits_file', xml_declaration=True, encoding='UTF-8',
            pretty_print=True)
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.Element', return_value=mock.Mock())
    @mock.patch('lxml.etree.ElementTree', return_value=mock.Mock())
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'zrange.return_value': [
                'limit0',
                'limit1',
                'limit2',
            ],
        })}))
    @mock.patch.object(limits.Limit, 'hydrate', side_effect=lambda x, y: y)
    @mock.patch.object(tools, 'make_limit_node')
    def test_altconf(self, mock_make_limit_node, mock_hydrate, mock_Config,
                     mock_loads, mock_ElementTree, mock_Element):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = dict(limits_key='alt_lims')
        db = conf.get_database.return_value
        root = mock_Element.return_value
        limit_tree = mock_ElementTree.return_value

        tools.dump_limits('conf_file', 'limits_file')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        db.zrange.assert_called_once_with('alt_lims', 0, -1)
        mock_loads.assert_has_calls([
            mock.call('limit0'),
            mock.call('limit1'),
            mock.call('limit2'),
        ])
        mock_hydrate.assert_has_calls([
            mock.call(db, 'limit0'),
            mock.call(db, 'limit1'),
            mock.call(db, 'limit2'),
        ])
        mock_Element.assert_called_once_with('limits')
        mock_ElementTree.assert_called_once_with(root)
        mock_make_limit_node.assert_has_calls([
            mock.call(root, 'limit0'),
            mock.call(root, 'limit1'),
            mock.call(root, 'limit2'),
        ])
        limit_tree.write.assert_called_once_with(
            'limits_file', xml_declaration=True, encoding='UTF-8',
            pretty_print=True)
        self.assertEqual(sys.stderr.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch('lxml.etree.Element', return_value=mock.Mock())
    @mock.patch('lxml.etree.ElementTree', return_value=mock.Mock())
    @mock.patch('msgpack.loads', side_effect=lambda x: x)
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'zrange.return_value': [
                'limit0',
                'limit1',
                'limit2',
            ],
        })}))
    @mock.patch.object(limits.Limit, 'hydrate', side_effect=lambda x, y: y)
    @mock.patch.object(tools, 'make_limit_node')
    def test_debug(self, mock_make_limit_node, mock_hydrate, mock_Config,
                   mock_loads, mock_ElementTree, mock_Element):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        root = mock_Element.return_value
        limit_tree = mock_ElementTree.return_value

        tools.dump_limits('conf_file', 'limits_file', debug=True)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        db.zrange.assert_called_once_with('limits', 0, -1)
        mock_loads.assert_has_calls([
            mock.call('limit0'),
            mock.call('limit1'),
            mock.call('limit2'),
        ])
        mock_hydrate.assert_has_calls([
            mock.call(db, 'limit0'),
            mock.call(db, 'limit1'),
            mock.call(db, 'limit2'),
        ])
        mock_Element.assert_called_once_with('limits')
        mock_ElementTree.assert_called_once_with(root)
        mock_make_limit_node.assert_has_calls([
            mock.call(root, 'limit0'),
            mock.call(root, 'limit1'),
            mock.call(root, 'limit2'),
        ])
        limit_tree.write.assert_called_once_with(
            'limits_file', xml_declaration=True, encoding='UTF-8',
            pretty_print=True)
        self.assertEqual(sys.stderr.getvalue(),
                         "Dumping limit index 0: 'limit0'\n"
                         "Dumping limit index 1: 'limit1'\n"
                         "Dumping limit index 2: 'limit2'\n"
                         "Dumping limits to file 'limits_file'\n")


class TestRemoteDaemon(unittest2.TestCase):
    @mock.patch('eventlet.monkey_patch')
    @mock.patch.object(config, 'Config', return_value=mock.Mock())
    @mock.patch.object(remote, 'RemoteControlDaemon', return_value=mock.Mock())
    def test_basic(self, mock_RemoteControlDaemon, mock_Config,
                   mock_monkey_patch):
        tools.remote_daemon('conf_file')

        mock_monkey_patch.assert_called_once_with()
        mock_Config.assert_called_once_with(conf_file='conf_file')
        mock_RemoteControlDaemon.assert_called_once_with(
            None, mock_Config.return_value)
        mock_RemoteControlDaemon.return_value.serve.assert_called_once_with()


class TestTurnstileCommand(unittest2.TestCase):
    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_basic(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'command')
        self.assertFalse(db.pubsub.called)
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_withargs(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD', ['arg1', 'arg2'])

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'command',
                                             'arg1', 'arg2')
        self.assertFalse(db.pubsub.called)
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_altconf(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = dict(channel='alt_chan')
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'alt_chan', 'command')
        self.assertFalse(db.pubsub.called)
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [{
                    'type': 'badtype',
                    'channel': 'response',
                    'data': 'test:bad:type',
                }, {
                    'type': 'pmessage',
                    'channel': 'badchan',
                    'data': 'test:bad:data:pmessage',
                }, {
                    'type': 'message',
                    'channel': 'badchan',
                    'data': 'test:bad:data:message',
                }, {
                    'type': 'pmessage',
                    'channel': 'response',
                    'data': 'test:one:two:three',
                }, {
                    'type': 'message',
                    'channel': 'response',
                    'data': 'test:four:five:six',
                }],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_listen(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD', channel='response')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'command')
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('response')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(),
                         'Response     1: test one two three\n'
                         'Response     2: test four five six\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [{
                    'type': 'badtype',
                    'channel': 'response',
                    'data': 'test:bad:type',
                }, {
                    'type': 'pmessage',
                    'channel': 'badchan',
                    'data': 'test:bad:data:pmessage',
                }, {
                    'type': 'message',
                    'channel': 'badchan',
                    'data': 'test:bad:data:message',
                }, {
                    'type': 'pmessage',
                    'channel': 'response',
                    'data': 'test:one:two:three',
                }, {
                    'type': 'message',
                    'channel': 'response',
                    'data': 'test:four:five:six',
                }],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_debug(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD', ['arg1', 'arg2'],
                                channel='response', debug=True)

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'command',
                                             'arg1', 'arg2')
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('response')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(),
                         'Issuing command: command arg1 arg2\n'
                         "Received message: {'channel': 'response', "
                         "'data': 'test:bad:type', "
                         "'type': 'badtype'}\n"
                         "Received message: {'channel': 'badchan', "
                         "'data': 'test:bad:data:pmessage', "
                         "'type': 'pmessage'}\n"
                         "Received message: {'channel': 'badchan', "
                         "'data': 'test:bad:data:message', "
                         "'type': 'message'}\n"
                         "Received message: {'channel': 'response', "
                         "'data': 'test:one:two:three', "
                         "'type': 'pmessage'}\n"
                         "Received message: {'channel': 'response', "
                         "'data': 'test:four:five:six', "
                         "'type': 'message'}\n")
        self.assertEqual(sys.stdout.getvalue(),
                         'Response     1: test one two three\n'
                         'Response     2: test four five six\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.side_effect': KeyboardInterrupt,
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_interrupt(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'CoMmAnD', channel='response')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'command')
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('response')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(), '')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [{
                    'type': 'message',
                    'channel': 'random_string',
                    'data': 'pong',
                }, {
                    'type': 'message',
                    'channel': 'random_string',
                    'data': 'pong:node',
                }, {
                    'type': 'message',
                    'channel': 'random_string',
                    'data': 'pong::1000000.0',
                }, {
                    'type': 'message',
                    'channel': 'random_string',
                    'data': 'pong:node:1000000.0',
                }],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_ping_basic(self, mock_command, mock_Config, mock_uuid4,
                        mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'ping')

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        mock_uuid4.assert_called_once_with()
        self.assertTrue(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'ping',
                                             'random_string', 1000000.0)
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('random_string')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(),
                         'Response     1: pong\n'
                         'Response     2: pong node\n'
                         'Response     3: pong  1000000.0 (RTT 3000.00ms)\n'
                         'Response     4: pong node 1000000.0 '
                         '(RTT 4000.00ms)\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [{
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong:node',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong::1000000.0',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong:node:1000000.0',
                }],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_ping_channel(self, mock_command, mock_Config, mock_uuid4,
                          mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'ping', ['pongchan'])

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertTrue(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'ping',
                                             'pongchan', 1000000.0)
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('pongchan')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(),
                         'Response     1: pong\n'
                         'Response     2: pong node\n'
                         'Response     3: pong  1000000.0 (RTT 3000.00ms)\n'
                         'Response     4: pong node 1000000.0 '
                         '(RTT 4000.00ms)\n')

    @mock.patch.object(sys, 'stderr', StringIO.StringIO())
    @mock.patch.object(sys, 'stdout', StringIO.StringIO())
    @mock.patch('time.time', side_effect=test_utils.TimeIncrementor(10))
    @mock.patch('uuid.uuid4', return_value='random_string')
    @mock.patch.object(config, 'Config', return_value=mock.MagicMock(**{
        'get_database.return_value': mock.Mock(**{
            'pubsub.return_value': mock.Mock(**{
                'listen.return_value': [{
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong:node',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong::1000000.0',
                }, {
                    'type': 'message',
                    'channel': 'pongchan',
                    'data': 'pong:node:1000000.0',
                }],
            }),
        })}))
    @mock.patch.object(database, 'command')
    def test_ping_echo(self, mock_command, mock_Config, mock_uuid4, mock_time):
        conf = mock_Config.return_value
        conf.__getitem__.return_value = {}
        db = conf.get_database.return_value
        pubsub = db.pubsub.return_value

        tools.turnstile_command('conf_file', 'ping', ['pongchan', 'echo'])

        mock_Config.assert_called_once_with(conf_file='conf_file')
        conf.get_database.assert_called_once_with()
        self.assertFalse(mock_uuid4.called)
        self.assertFalse(mock_time.called)
        mock_command.assert_called_once_with(db, 'control', 'ping',
                                             'pongchan', 'echo')
        db.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('pongchan')
        pubsub.listen.assert_called_once_with()
        self.assertEqual(sys.stderr.getvalue(), '')
        self.assertEqual(sys.stdout.getvalue(),
                         'Response     1: pong\n'
                         'Response     2: pong node\n'
                         'Response     3: pong  1000000.0\n'
                         'Response     4: pong node 1000000.0\n')


class TestCompactorDaemon(unittest2.TestCase):
    @mock.patch('eventlet.monkey_patch')
    @mock.patch.object(config, 'Config', return_value=mock.Mock())
    @mock.patch.object(compactor, 'compactor')
    def test_basic(self, mock_compactor, mock_Config, mock_monkey_patch):
        tools.compactor_daemon('conf_file')

        mock_monkey_patch.assert_called_once_with()
        mock_Config.assert_called_once_with(conf_file='conf_file')
        mock_compactor.assert_called_once_with(mock_Config.return_value)
