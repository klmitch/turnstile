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
