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

import pkg_resources


def import_class(import_str):
    """
    Returns a class from a string including module and class.

    Note: No sanity check is performed to ensure that the imported
    symbol truly is a class.

    :param import_str: The string to import.  Consists of a module
                       name and the name of the class in that module,
                       separated by a colon (':').

    :returns: The desired class.
    """

    try:
        return pkg_resources.EntryPoint.parse("x=" + import_str).load(False)
    except (ImportError, pkg_resources.UnknownExtra) as exc:
        # Convert it into an import error
        raise ImportError("Failed to import %s: %s" % (import_str, exc))


def find_entrypoint(group, name):
    """
    Finds the first available entrypoint with the given name in the
    given group.

    :param group: The entrypoint group the name can be found in.
    :param name: The name of the entrypoint.

    :returns: The entrypoint object, or None if one could not be
              loaded.
    """

    for ep in pkg_resources.iter_entry_points(group, name):
        try:
            # Load and return the object
            return ep.load()
        except (ImportError, pkg_resources.UnknownExtra):
            # Couldn't load it; try the next one
            continue

    # Couldn't find one...
    return None


class ignore_except(object):
    """Context manager to ignore all exceptions."""

    def __enter__(self):
        """Entry does nothing."""

        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Return True to mark the exception as handled."""

        return True
