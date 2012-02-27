import ConfigParser
import StringIO
import warnings

import argparse
from lxml import etree
import msgpack

from turnstile import database
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


class FakeConfigParser(object):
    _cfg_options = dict(
        bad_config={},
        good_config=dict(
            connection={},
            ),
        alt_limits=dict(
            connection=dict(
                limits_key='alternate',
                ),
            ),
        alt_control = dict(
            connection=dict(
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


class FakeArgumentParser(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeArgumentParser, self).__init__(*args, **kwargs)

        self.arguments = []

    def add_argument(self, *args, **kwargs):
        self.arguments.append((args, kwargs))

    def parse_args(self):
        return FakeNamespace()


class FakeArgumentParserDebug(FakeArgumentParser):
    def parse_args(self):
        ns = super(FakeArgumentParserDebug, self).parse_args()
        ns.debug = True
        return ns


class BaseToolTest(tests.TestCase):
    def setUp(self):
        super(BaseToolTest, self).setUp()

        self.config = None
        self.fakedb = FakeDatabase()

        def fake_initialize(cfg):
            self.config = cfg
            return self.fakedb

        self.stubs.Set(database, 'initialize', fake_initialize)
        self.stubs.Set(ConfigParser, 'SafeConfigParser', FakeConfigParser)


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

    def test_parse_list_attr_empty(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="list_attr">spam</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Limit at index 1: Missing child elements for "
                          "attribute 'list_attr'", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                list_attr=[],
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

    def test_parse_dict_attr_empty(self):
        limit_xml = """<limit class="FakeLimit">
    <attr name="dict_attr">spam</attr>
    <attr name="required">spam</attr>
</limit>"""
        limit_node = etree.fromstring(limit_xml)

        with warnings.catch_warnings(record=True) as w:
            limit = tools.parse_limit_node('db', 1, limit_node)

            self.assertEqual(len(w), 1)
            self.assertIn("Limit at index 1: Missing child elements for "
                          "attribute 'dict_attr'", w[-1].message)

        self.assertIsInstance(limit, FakeLimit)
        self.assertEqual(limit.args, ('db',))
        self.assertEqual(limit.kwargs, dict(
                required='spam',
                dict_attr={},
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
