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

from lxml import etree
import mock
import unittest2

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
        self.assertEqual(sa._arguments, [])
        self.assertEqual(sa.description, 'this is a test')

    @mock.patch('functools.update_wrapper', side_effect=lambda x, y: x)
    def test_wrap_unwrapped(self, mock_update_wrapper):
        func = mock.Mock(__doc__="\n    this is\n    a test  \n\n    of this")

        sa = tools.ScriptAdaptor._wrap(func)

        self.assertIsInstance(sa, tools.ScriptAdaptor)
        self.assertEqual(sa._func, func)
        self.assertEqual(sa._preprocess, [])
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

        result = sa.console()

        self.assertEqual(result, 'result')
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
