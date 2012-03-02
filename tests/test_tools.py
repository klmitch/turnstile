import ConfigParser
import StringIO
import sys
import warnings

import argparse
from lxml import etree
import msgpack

from turnstile import database
from turnstile import limits
from turnstile import tools

import tests
from tests.test_database import FakeDatabase


class FakeLimit(tests.GenericFakeClass):
    attrs = dict(
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


class FakeConfigParser(object):
    _cfg_options = dict(
        bad_config={},
        good_config=dict(
            connection=dict(
                host='example.com',
                ),
            ),
        alt_limits=dict(
            connection=dict(
                host='example.com',
                limits_key='alternate',
                ),
            ),
        alt_control=dict(
            connection=dict(
                host='example.com',
                control_channel='alternate',
                ),
            ),
        )

    def __init__(self):
        self.config = None

    def read(self, cfg_file):
        self.config = self._cfg_options[cfg_file]

    def has_section(self, section):
        return section in self.config

    def items(self, section):
        return self.config[section].items()


class FakeNamespace(object):
    config = 'config'
    limits_file = 'limits.xml'
    reload = False
    dry_run = True
    debug = False


class FakeArgumentParser(object):
    def __init__(self, *args, **kwargs):
        pass

    def add_argument(self, *args, **kwargs):
        pass

    def parse_args(self):
        return FakeNamespace()


class FakeArgumentParserDebug(FakeArgumentParser):
    def parse_args(self):
        ns = super(FakeArgumentParserDebug, self).parse_args()
        ns.debug = True
        return ns


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

    def stub_argparse(self, debug=False):
        cls = FakeArgumentParserDebug if debug else FakeArgumentParser
        self.stubs.Set(argparse, 'ArgumentParser', cls)


class TestConsoleSetupLimits(ConsoleScriptsTestCase):
    subroutine = staticmethod(tools.setup_limits)

    def test_basic(self):
        self.stub_argparse()
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, False), {}))

    def test_exception(self):
        self.stub_argparse()
        self.result = Exception("An error occurred")
        res = self.subroutine()

        self.assertEqual(res, "An error occurred")
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, False), {}))

    def test_exception_debug(self):
        self.stub_argparse(True)
        self.result = Exception("An error occurred")

        with self.assertRaises(Exception):
            res = self.subroutine()

        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False, True, True), {}))


class TestConsoleDumpLimits(ConsoleScriptsTestCase):
    subroutine = staticmethod(tools.dump_limits)

    def test_basic(self):
        self.stub_argparse()
        res = self.subroutine()

        self.assertEqual(res, None)
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False), {}))

    def test_exception(self):
        self.stub_argparse()
        self.result = Exception("An error occurred")
        res = self.subroutine()

        self.assertEqual(res, "An error occurred")
        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', False), {}))

    def test_exception_debug(self):
        self.stub_argparse(True)
        self.result = Exception("An error occurred")

        with self.assertRaises(Exception):
            res = self.subroutine()

        self.assertEqual(self.subargs,
                         (('config', 'limits.xml', True), {}))


class TestParseConfig(tests.TestCase):
    def setUp(self):
        super(TestParseConfig, self).setUp()

        def fake_initialize(cfg):
            return cfg

        self.stubs.Set(database, 'initialize', fake_initialize)
        self.stubs.Set(ConfigParser, 'SafeConfigParser', FakeConfigParser)

    def test_basic(self):
        result = tools.parse_config('good_config')

        self.assertEqual(result,
                         (dict(host='example.com'), 'limits', 'control'))

    def test_alt_limits(self):
        result = tools.parse_config('alt_limits')

        self.assertEqual(result,
                         (dict(host='example.com'), 'alternate', 'control'))

    def test_alt_control(self):
        result = tools.parse_config('alt_control')

        self.assertEqual(result,
                         (dict(host='example.com'), 'limits', 'alternate'))

    def test_missing_connection(self):
        with self.assertRaises(Exception):
            result = tools.parse_config('bad_config')


class BaseToolTest(tests.TestCase):
    def setUp(self):
        super(BaseToolTest, self).setUp()

        self.fakedb = FakeDatabase()
        self.stderr = StringIO.StringIO()

        def fake_parse_config(config):
            self.assertEqual(config, 'config.file')

            return self.fakedb, 'limits', 'control'

        self.stubs.Set(tools, 'parse_config', fake_parse_config)
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
        self.stubs.Set(FakeDatabase, 'limit_update', fake_limit_update)
        self.stubs.Set(FakeDatabase, 'command', fake_command)

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
