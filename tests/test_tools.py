import logging
import logging.config
import StringIO
import sys
import warnings

import argparse
import eventlet
from lxml import etree
import msgpack

from turnstile import config
from turnstile import limits
from turnstile import remote
from turnstile import tools

import tests
from tests import db_fixture


class FakeLimit(tests.GenericFakeClass):
    attrs = dict(
        bool_attr=dict(
            type=bool,
            default=False,
            ),
        int_attr=dict(
            type=int,
            default=1,
            ),
        float_attr=dict(
            type=float,
            default=1.5,
            ),
        str_attr=dict(
            type=str,
            default='spam',
            ),
        list_attr=dict(
            type=list,
            subtype=str,
            default=lambda: [],
            ),
        dict_attr=dict(
            type=dict,
            subtype=str,
            default=lambda: {},
            ),
        required=dict(
            type=str,
            ),
        )

    _limit_full_name = "FakeLimit"

    @classmethod
    def hydrate(cls, db, limit):
        return cls(db, **limit)

    def dehydrate(self):
        return self.kwargs

    def __getattr__(self, attr):
        return self.kwargs[attr]

    def __repr__(self):
        return repr(self.kwargs)


class FakeNamespace(object):
    def __init__(self, params):
        self._params = params

    def __getattr__(self, name):
        return self._params[name]


class FakeArgumentParser(object):
    def __init__(self, params):
        self._params = params

    def add_argument(self, *args, **kwargs):
        pass

    def parse_args(self):
        return FakeNamespace(self._params)


class FakeConfig(object):
    def __init__(self, control):
        self._control = control
        self._called = []

    def get_database(self):
        self._called.append(('get_database',))
        return 'database'

    def __getitem__(self, name):
        if name != 'control':
            raise KeyError(name)
        return self._control


class TestParseConfig(tests.TestCase):
    def setUp(self):
        super(TestParseConfig, self).setUp()

        self.control = {}

        def get_config(**kwargs):
            self.assertEqual(kwargs, dict(conf_file='conf_file'))
            return FakeConfig(self.control)

        self.stubs.Set(config, 'Config', get_config)

    def test_parse_config(self):
        result = tools.parse_config('conf_file')

        self.assertEqual(result, ('database', 'limits', 'control'))

    def test_parse_config_alt(self):
        self.control = dict(limits_key='alt_limits', channel='alt_control')

        result = tools.parse_config('conf_file')

        self.assertEqual(result, ('database', 'alt_limits', 'alt_control'))


class TestParseLimitNode(tests.TestCase):
    imports = {
        'FakeLimit': FakeLimit,
        }

    def test_parse_basic(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(required='spam'))

    def test_parse_missing_requirement(self):
        limit_xml = '<limit class="FakeLimit"></limit>'
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            with self.assertRaises(TypeError):
                limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

    def test_parse_unknown_elem(self):
        limit_xml = """<limit class="FakeLimit">
    <attribute name="badelem">spam</attribute>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Unrecognized element 'attribute' while parsing "
                          "limit at index 1; ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(required='spam'))

    def test_parse_unknown_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="unknown_attr">spam</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Limit at index 1 does not accept an attribute "
                          "'unknown_attr'; ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(required='spam'))

    def test_parse_bool_attr_true(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="bool_attr">True</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                bool_attr=True,
                ))

    def test_parse_bool_attr_false(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="bool_attr">False</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                bool_attr=False,
                ))

    def test_parse_bool_attr_unknown(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="bool_attr">unknown</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Unrecognized boolean value 'unknown' while parsing "
                          "'bool_attr' attribute of limit at index 1; "
                          "ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                ))

    def test_parse_int_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="int_attr">5</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                int_attr=5,
                ))

    def test_parse_float_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="float_attr">3.14</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                float_attr=3.14,
                ))

    def test_parse_str_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="str_attr">ni</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                str_attr='ni',
                ))

    def test_parse_list_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="list_attr">
        <value>spam</value>
        <value>ni</value>
    </attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                list_attr=['spam', 'ni'],
                ))

    def test_parse_list_attr_unknown_elem(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="list_attr">
        <val>spam</val>
        <value>ni</value>
    </attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Unrecognized element 'val' while parsing "
                          "'list_attr' attribute of limit at index 1; "
                          "ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                list_attr=['ni'],
                ))

    def test_parse_dict_attr(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="dict_attr">
        <value key="foo">spam</value>
        <value key="bar">ni</value>
    </attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 0)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                dict_attr=dict(foo='spam', bar='ni'),
                ))

    def test_parse_dict_attr_unknown_elem(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="dict_attr">
        <val key="foo">spam</val>
        <value key="bar">ni</value>
    </attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Unrecognized element 'val' while parsing "
                          "'dict_attr' attribute of limit at index 1; "
                          "ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                dict_attr=dict(bar='ni'),
                ))

    def test_parse_dict_attr_missing_key(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="dict_attr">
        <value>spam</value>
        <value key="bar">ni</value>
    </attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Missing 'key' attribute of 'value' element "
                          "while parsing 'dict_attr' attribute of limit "
                          "at index 1; ignoring...", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                dict_attr=dict(bar='ni'),
                ))


class TestMakeLimitNode(tests.TestCase):
    def setUp(self):
        super(TestMakeLimitNode, self).setUp()

        self.root = etree.Element('root')
        self.limit_kwargs = dict(
            bool_attr=False,
            int_attr=1,
            float_attr=1.5,
            str_attr='spam',
            list_attr=[],
            dict_attr={},
            required='required',
            )

    def _match_xml(self, expected):
        actual = etree.tostring(self.root)
        self.assertEqual(actual, '<root>%s</root>' % expected)

    def _prepare_limit(self, **kwargs):
        self.limit_kwargs.update(kwargs)
        return FakeLimit(**self.limit_kwargs)

    def test_make_basic(self):
        limit = self._prepare_limit()
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="required">required</attr>'
                        '</limit>')

    def test_make_int_attr(self):
        limit = self._prepare_limit(int_attr=5)
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="int_attr">5</attr>'
                        '<attr name="required">required</attr>'
                        '</limit>')

    def test_make_float_attr(self):
        limit = self._prepare_limit(float_attr=3.14)
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="float_attr">3.14</attr>'
                        '<attr name="required">required</attr>'
                        '</limit>')

    def test_make_str_attr(self):
        limit = self._prepare_limit(str_attr="nospam")
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="required">required</attr>'
                        '<attr name="str_attr">nospam</attr>'
                        '</limit>')

    def test_make_list_attr(self):
        limit = self._prepare_limit(list_attr=['spam', 'ni'])
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="list_attr">'
                        '<value>spam</value>'
                        '<value>ni</value>'
                        '</attr>'
                        '<attr name="required">required</attr>'
                        '</limit>')

    def test_make_dict_attr(self):
        limit = self._prepare_limit(dict_attr=dict(foo='spam', bar='ni'))
        tools.make_limit_node(self.root, limit)

        self._match_xml('<limit class="FakeLimit">'
                        '<attr name="dict_attr">'
                        '<value key="bar">ni</value>'
                        '<value key="foo">spam</value>'
                        '</attr>'
                        '<attr name="required">required</attr>'
                        '</limit>')


class ConsoleScriptsTestCase(tests.TestCase):
    def setUp(self):
        super(ConsoleScriptsTestCase, self).setUp()

        self.result = None
        self.subargs = None

        def fake_subroutine(*args, **kwargs):
            self.subargs = (args, kwargs)

            if isinstance(self.result, Exception):
                raise self.result
            return self.result

        self.stubs.Set(tools, '_%s' % self.subroutine.__name__,
                       fake_subroutine)

    def stub_argparse(self, **params):
        def fake_ArgumentParser(*args, **kwargs):
            return FakeArgumentParser(params)
        self.stubs.Set(argparse, 'ArgumentParser', fake_ArgumentParser)


class TestConsoleSetupLimits(ConsoleScriptsTestCase):
    subroutine = staticmethod(tools.setup_limits)

    def test_basic(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           reload=False, dry_run=True, debug=False)
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, False), {}))

    def test_exception(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           reload=False, dry_run=True, debug=False)
        self.result = Exception("An error occurred")
        res = self.subroutine()

        self.assertEqual(res, "An error occurred")
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, False), {}))

    def test_exception_debug(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           reload=False, dry_run=True, debug=True)
        self.result = Exception("An error occurred")

        with self.assertRaises(Exception):
            res = self.subroutine()

        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, True), {}))


class TestConsoleDumpLimits(ConsoleScriptsTestCase):
    subroutine = staticmethod(tools.dump_limits)

    def test_basic(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           debug=False)
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False), {}))

    def test_exception(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           debug=False)
        self.result = Exception("An error occurred")
        res = self.subroutine()

        self.assertEqual(res, "An error occurred")
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False), {}))

    def test_exception_debug(self):
        self.stub_argparse(config='config', limits_file='limits.xml',
                           debug=True)
        self.result = Exception("An error occurred")

        with self.assertRaises(Exception):
            res = self.subroutine()

        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', True), {}))


class TestConsoleRemoteDaemon(ConsoleScriptsTestCase):
    subroutine = staticmethod(tools.remote_daemon)

    def setUp(self):
        super(TestConsoleRemoteDaemon, self).setUp()

        self.logging_config = None
        self.logging_basic = False

        def fake_fileConfig(filename):
            self.logging_config = filename

        def fake_basicConfig():
            self.logging_basic = True

        self.stubs.Set(logging.config, 'fileConfig', fake_fileConfig)
        self.stubs.Set(logging, 'basicConfig', fake_basicConfig)

    def test_basic(self):
        self.stub_argparse(config='config', logging=None, debug=False)
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs, (('config',), {}))
        self.assertEqual(self.logging_config, None)
        self.assertEqual(self.logging_basic, True)

    def test_logging(self):
        self.stub_argparse(config='config', logging='log.conf', debug=False)
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs, (('config',), {}))
        self.assertEqual(self.logging_config, 'log.conf')
        self.assertEqual(self.logging_basic, False)

    def test_exception(self):
        self.stub_argparse(config='config', logging=None, debug=False)
        self.result = Exception("An error occurred")
        res = self.subroutine()

        self.assertEqual(res, "An error occurred")
        self.assertEqual(self.subargs, (('config',), {}))
        self.assertEqual(self.logging_config, None)
        self.assertEqual(self.logging_basic, True)

    def test_exception_debug(self):
        self.stub_argparse(config='config', logging=None, debug=True)
        self.result = Exception("An error occurred")

        with self.assertRaises(Exception):
            res = self.subroutine()

        self.assertEqual(self.subargs, (('config',), {}))
        self.assertEqual(self.logging_config, None)
        self.assertEqual(self.logging_basic, True)


class BaseToolTest(tests.TestCase):
    def setUp(self):
        super(BaseToolTest, self).setUp()

        self.fakedb = db_fixture.FakeDatabase()
        self.stderr = StringIO.StringIO()

        class FakeConfig(config.Config):
            def __init__(inst, conf_dict=None, conf_file=None):
                inst._config = {
                    'control': {
                        'limits_key': 'limits',
                        'channel': 'control',
                        },
                    }

            def get_database(inst, override=None):
                return self.fakedb

        self.stubs.Set(config, 'Config', FakeConfig)
        self.stubs.Set(sys, 'stderr', self.stderr)


class TestToolSetupLimits(BaseToolTest):
    def setUp(self):
        super(TestToolSetupLimits, self).setUp()

        self.fail_idx = None
        self.parsed = []
        self.lims = None
        self.cmds = []

        def fake_parse_limit_node(db, idx, lim):
            lim_id = lim.get('id')

            self.parsed.append((idx, lim_id))
            if idx == self.fail_idx:
                raise Exception("Failed to parse")

            return "Limit %s" % lim_id

        def fake_limit_update(db, limits_key, lims):
            self.assertEqual(limits_key, 'limits')
            self.lims = lims

        def fake_command(db, control_channel, command, *params):
            self.assertEqual(control_channel, 'control')
            self.cmds.append((command, params))

        self.stubs.Set(tools, 'parse_limit_node', fake_parse_limit_node)
        self.stubs.Set(db_fixture.FakeDatabase, 'limit_update',
                       fake_limit_update)
        self.stubs.Set(db_fixture.FakeDatabase, 'command', fake_command)

    def test_basic(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ["Limit 0", "Limit 1"])
        self.assertEqual(self.cmds, [("reload", ())])
        self.assertEqual(self.stderr.getvalue(), '')

    def test_basic_debug(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, debug=True)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ["Limit 0", "Limit 1"])
        self.assertEqual(self.cmds, [("reload", ())])
        self.assertEqual(self.stderr.getvalue(),
                         "Installing the following limits:\n"
                         "  'Limit 0'\n"
                         "  'Limit 1'\n"
                         "Issuing command: reload\n")

    def test_basic_dryrun(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, dry_run=True)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, None)
        self.assertEqual(self.cmds, [])
        self.assertEqual(self.stderr.getvalue(),
                         "Installing the following limits:\n"
                         "  'Limit 0'\n"
                         "  'Limit 1'\n"
                         "Issuing command: reload\n")

    def test_reload_false(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, do_reload=False)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ['Limit 0', 'Limit 1'])
        self.assertEqual(self.cmds, [])
        self.assertEqual(self.stderr.getvalue(), "")

    def test_reload_string(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, do_reload='spam')

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ['Limit 0', 'Limit 1'])
        self.assertEqual(self.cmds, [('reload', ('spam',))])
        self.assertEqual(self.stderr.getvalue(), "")

    def test_reload_int(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, do_reload=5)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ['Limit 0', 'Limit 1'])
        self.assertEqual(self.cmds, [('reload', ('spread', 5))])
        self.assertEqual(self.stderr.getvalue(), "")

    def test_reload_float(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file, do_reload=3.14)

            self.assertEqual(len(w), 0)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1")])
        self.assertEqual(self.lims, ['Limit 0', 'Limit 1'])
        self.assertEqual(self.cmds, [('reload', ('spread', 3.14))])
        self.assertEqual(self.stderr.getvalue(), "")

    def test_bad_tag(self):
        limits_file = StringIO.StringIO("""<limits>
    <timil id="0"/>
    <limit id="1"/>
</limits>""")

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file)

            self.assertEqual(len(w), 1)
            self.assertIn("Unrecognized tag 'timil' in limits file at index 0",
                          w[-1].message)

        self.assertEqual(self.parsed, [(1, "1")])
        self.assertEqual(self.lims, ['Limit 1'])
        self.assertEqual(self.cmds, [('reload', ())])
        self.assertEqual(self.stderr.getvalue(), "")

    def test_bad_limit(self):
        limits_file = StringIO.StringIO("""<limits>
    <limit id="0"/>
    <limit id="1"/>
    <limit id="2"/>
</limits>""")
        self.fail_idx = 1

        with warnings.catch_warnings(record=True) as w:
            tools._setup_limits('config.file', limits_file)

            self.assertEqual(len(w), 1)
            self.assertIn("Couldn't understand limit at index 1: Failed to "
                          "parse", w[-1].message)

        self.assertEqual(self.parsed, [(0, "0"), (1, "1"), (2, "2")])
        self.assertEqual(self.lims, ['Limit 0', 'Limit 2'])
        self.assertEqual(self.cmds, [('reload', ())])
        self.assertEqual(self.stderr.getvalue(), "")


class TestToolDumpLimits(BaseToolTest):
    def setUp(self):
        super(TestToolDumpLimits, self).setUp()

        self.fakedb._fakedb['limits'] = [
            (10, dict(name="limit1")),
            (20, dict(name="limit2")),
            (30, dict(name="limit3")),
            ]

        self.limits_file = StringIO.StringIO()

        def fake_make_limit_node(root, lim):
            etree.SubElement(root, 'limit', name=lim.name)

        self.stubs.Set(tools, 'make_limit_node', fake_make_limit_node)
        self.stubs.Set(msgpack, 'loads', lambda x: x)
        self.stubs.Set(limits, 'Limit', FakeLimit)

    def test_basic(self):
        tools._dump_limits('config.file', self.limits_file)

        self.assertEqual(self.limits_file.getvalue(),
                         """<?xml version='1.0' encoding='UTF-8'?>
<limits>
  <limit name="limit1"/>
  <limit name="limit2"/>
  <limit name="limit3"/>
</limits>
""")
        self.assertEqual(self.stderr.getvalue(), "")

    def test_debug(self):
        tools._dump_limits('config.file', self.limits_file, debug=True)

        self.assertEqual(self.limits_file.getvalue(),
                         """<?xml version='1.0' encoding='UTF-8'?>
<limits>
  <limit name="limit1"/>
  <limit name="limit2"/>
  <limit name="limit3"/>
</limits>
""")
        self.assertTrue(self.stderr.getvalue().startswith(
                """Dumping limit index 0: {'name': 'limit1'}
Dumping limit index 1: {'name': 'limit2'}
Dumping limit index 2: {'name': 'limit3'}
Dumping limits to file """))


class TestToolRemoteDaemon(BaseToolTest):
    def setUp(self):
        super(TestToolRemoteDaemon, self).setUp()

        self.served = False
        self.daemon = None
        self.monkey_patched = False

        class FakeRemoteControlDaemon(tests.GenericFakeClass):
            def __init__(inst, *args, **kwargs):
                super(FakeRemoteControlDaemon, inst).__init__(*args, **kwargs)
                self.daemon = inst

            def serve(inst):
                self.served = True

        def fake_monkey_patch():
            self.monkey_patched = True

        self.stubs.Set(remote, 'RemoteControlDaemon', FakeRemoteControlDaemon)
        self.stubs.Set(eventlet, 'monkey_patch', fake_monkey_patch)

    def test_basic(self):
        with warnings.catch_warnings(record=True) as w:
            tools._remote_daemon('config.file')

            self.assertEqual(len(w), 0)

        self.assertEqual(self.monkey_patched, True)
        self.assertEqual(self.served, True)
        self.assertEqual(self.daemon.args[0], None)
        self.assertIsInstance(self.daemon.args[1], config.Config)
