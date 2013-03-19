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

from lxml import etree


class TestException(Exception):
    pass


class Halt(BaseException):
    pass


class XMLMatchState(object):
    """
    Maintain some state for matching.

    Tracks the XML node path and saves the expected and actual full
    XML text, for use by the XMLMismatch subclasses.
    """

    def __init__(self):
        self.path = []

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.path.pop()
        return False

    def __str__(self):
        return '/' + '/'.join(self.path)

    def node(self, tag, idx):
        """
        Adds tag and index to the path; they will be popped off when
        the corresponding 'with' statement exits.

        :param tag: The element tag
        :param idx: If not None, the integer index of the element
                    within its parent.  Not included in the path
                    element if None.
        """

        if idx is not None:
            self.path.append("%s[%d]" % (tag, idx))
        else:
            self.path.append(tag)
        return self


def _compare_node(expected, actual, state, idx):
    """Recursively compares nodes within the XML tree."""

    # Start by comparing the tags
    if expected.tag != actual.tag:
        raise AssertionError("s: XML tag mismatch at index %d: "
                             "expected tag <%s>; actual tag <%s>" %
                             (state, idx, expected.tag, actual.tag))

    with state.node(expected.tag, idx):
        # Compare the attribute keys
        expected_attrs = set(expected.attrib.keys())
        actual_attrs = set(actual.attrib.keys())
        if expected_attrs != actual_attrs:
            expected = ', '.join(sorted(expected_attrs - actual_attrs))
            actual = ', '.join(sorted(actual_attrs - expected_attrs))
            raise AssertionError("%s: XML attributes mismatch: "
                                 "keys only in expected: %s; "
                                 "keys only in actual: %s" %
                                 (state, expected, actual))

        # Compare the attribute values
        for key in expected_attrs:
            expected_value = expected.attrib[key]
            actual_value = actual.attrib[key]

            if 'DONTCARE' in (expected_value, actual_value):
                continue
            elif expected_value != actual_value:
                raise AssertionError("%s: XML attribute value mismatch: "
                                     "expected value of attribute %s: %r; "
                                     "actual value: %r" %
                                     (state, key, expected_value,
                                      actual_value))

        # Compare the contents of the node
        if len(expected) == 0 and len(actual) == 0:
            # No children, compare text values
            if ('DONTCARE' not in (expected.text, actual.text) and
                    expected.text != actual.text):
                raise AssertionError("%s: XML text value mismatch: "
                                     "expected text value: %r; "
                                     "actual value: %r" %
                                     (state, expected.text, actual.text))
        else:
            expected_idx = 0
            actual_idx = 0
            while (expected_idx < len(expected) and
                   actual_idx < len(actual)):
                # Ignore comments and processing instructions
                # TODO(Vek): may interpret PIs in the future, to
                # allow for, say, arbitrary ordering of some
                # elements
                if (expected[expected_idx].tag in
                        (etree.Comment, etree.ProcessingInstruction)):
                    expected_idx += 1
                    continue

                # Compare the nodes
                result = _compare_node(expected[expected_idx],
                                       actual[actual_idx], state,
                                       actual_idx)
                if result is not True:
                    return result

                # Step on to comparing the next nodes...
                expected_idx += 1
                actual_idx += 1

            # Make sure we consumed all nodes in actual
            if actual_idx < len(actual):
                raise AssertionError("%s: XML unexpected child element "
                                     "<%s> present at index %d" %
                                     (state, actual[actual_idx].tag,
                                      actual_idx))

            # Make sure we consumed all nodes in expected
            if expected_idx < len(expected):
                for node in expected[expected_idx:]:
                    if (node.tag in
                            (etree.Comment, etree.ProcessingInstruction)):
                        continue

                    raise AssertionError("%s: XML expected child element "
                                         "<%s> not present at index %d" %
                                         (state, node.tag, actual_idx))

    # The nodes match
    return True


def compare_xml(expected, actual):
    """Compare two XML strings."""

    expected = etree.fromstring(expected)
    if isinstance(actual, basestring):
        actual = etree.fromstring(actual)

    state = XMLMatchState()
    result = _compare_node(expected, actual, state, None)

    if result is False:
        raise AssertionError("%s: XML does not match" % state)
    elif result is not True:
        return result


class TimeIncrementor(object):
    def __init__(self, interval, start=1000000.0):
        self.time = start - interval
        self.interval = interval

    def __call__(self):
        self.time += self.interval
        return self.time
