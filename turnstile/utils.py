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


def find_entrypoint(group, name, compat=True, required=False):
    """
    Finds the first available entrypoint with the given name in the
    given group.

    :param group: The entrypoint group the name can be found in.  If
                  None, the name is not presumed to be an entrypoint.
    :param name: The name of the entrypoint.
    :param compat: If True, and if the name parameter contains a ':',
                   the name will be interpreted as a module name and
                   an object name, separated by a colon.  This is
                   provided for compatibility.
    :param required: If True, and no corresponding entrypoint can be
                     found, an ImportError will be raised.  If False
                     (the default), None will be returned instead.

    :returns: The entrypoint object, or None if one could not be
              loaded.
    """

    if group is None or (compat and ':' in name):
        try:
            return pkg_resources.EntryPoint.parse("x=" + name).load(False)
        except (ImportError, pkg_resources.UnknownExtra) as exc:
            pass
    else:
        for ep in pkg_resources.iter_entry_points(group, name):
            try:
                # Load and return the object
                return ep.load()
            except (ImportError, pkg_resources.UnknownExtra):
                # Couldn't load it; try the next one
                continue

    # Raise an ImportError if requested
    if required:
        raise ImportError("Cannot import %r entrypoint %r" % (group, name))

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
