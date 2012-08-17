# Copyright 2012 Rackspace
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

import sys


def import_class(import_str):
    """Returns a class from a string including module and class."""

    mod_str, _sep, class_str = import_str.rpartition(':')
    try:
        __import__(mod_str)
        return getattr(sys.modules[mod_str], class_str)
    except (ImportError, ValueError, AttributeError) as exc:
        # Convert it into an import error
        raise ImportError("Failed to import %s: %s" % (import_str, exc))


class ignore_except(object):
    """Context manager to ignore all exceptions."""

    def __enter__(self):
        """Entry does nothing."""

        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Return True to mark the exception as handled."""

        return True


_str_true = set(['t', 'true', 'on', 'y', 'yes'])
_str_false = set(['f', 'false', 'off', 'n', 'no'])


def to_bool(value, do_raise=True):
    """Convert a string to a boolean value.

    If the string consists of digits, the integer value of the string
    is coerced to a boolean value.  Otherwise, any of the strings "t",
    "true", "on", "y", and "yes" are considered True and any of the
    strings "f", "false", "off", "n", and "no" are considered False.
    A ValueError will be raised for any other value.
    """

    value = value.lower()

    # Try it as an integer
    if value.isdigit():
        return bool(int(value))

    # OK, check it against the true/false values...
    if value in _str_true:
        return True
    elif value in _str_false:
        return False

    # Not recognized
    if do_raise:
        raise ValueError("invalid literal for to_bool(): %r" % value)

    return False
