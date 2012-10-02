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

import logging
import logging.config
import sys
import warnings

import argparse
import eventlet
from lxml import etree
import msgpack

from turnstile import config
from turnstile import limits
from turnstile import remote
from turnstile import utils


def parse_config(conf_file):
    """
    Provide backwards-compatibility.

    Previous versions of Turnstile had parse_config(), which would
    parse a Turnstile configuration file and return a database handle,
    the limits key, and the control channel, as a tuple.  As it turns
    out, some external tools referenced this parse_config() function.
    This function, unused by Turnstile itself, provides backwards
    compatibility for this functionality, using the new config.Config
    class.
    """

    # Read the configuration file
    conf = config.Config(conf_file=conf_file)

    # Get a database handle and the limits key and control channel
    db = conf.get_database()
    limits_key = conf['control'].get('limits_key', 'limits')
    control_channel = conf['control'].get('channel', 'control')

    return db, limits_key, control_channel


def parse_limit_node(db, idx, limit):
    """
    Given an XML node describing a limit, return a Limit object.

    :param db: Handle for the Redis database.
    :param idx: The index of the limit in the XML file; used for error
                reporting.
    :param limit: The XML node describing the limit.
    """

    # First, try to import the class; this will raise ImportError if
    # we can't import it
    klass = utils.import_class(limit.get('class'))

    # Build the list of required attributes
    required = set(k for k, v in klass.attrs.items()
                   if 'default' not in v)

    # Now, use introspection on the class to interpret the attributes
    attrs = {}
    for child in limit:
        # Basic validation of child elements
        if child.tag != 'attr':
            warnings.warn("Unrecognized element %r while parsing limit at "
                          "index %d; ignoring..." % (child.tag, idx))
            continue

        # Get the attribute name
        attr = child.get('name')

        # Be liberal in what we accept--ignore unrecognized attributes
        # (with a warning)
        if attr not in klass.attrs:
            warnings.warn("Limit at index %d does not accept an attribute "
                          "%r; ignoring..." % (idx, attr))
            continue

        # OK, get the attribute descriptor
        desc = klass.attrs[attr]

        # Grab the attribute type
        attr_type = desc.get('type', str)

        if attr_type == list:
            # Lists are expressed as child elements; we ignore the
            # child element names
            subtype = desc.get('subtype', str)
            value = []
            for grandchild in child:
                if grandchild.tag != 'value':
                    warnings.warn("Unrecognized element %r while parsing "
                                  "%r attribute of limit at index %d; "
                                  "ignoring..." %
                                  (grandchild.tag, attr, idx))
                    continue

                value.append(subtype(grandchild.text))
        elif attr_type == dict:
            # Dicts are expressed as child elements, with the tags
            # identifying the attribute name
            subtype = desc.get('subtype', str)
            value = {}
            for grandchild in child:
                if grandchild.tag != 'value':
                    warnings.warn("Unrecognized element %r while parsing "
                                  "%r attribute of limit at index %d; "
                                  "ignoring..." %
                                  (grandchild.tag, attr, idx))
                    continue
                elif 'key' not in grandchild.attrib:
                    warnings.warn("Missing 'key' attribute of 'value' "
                                  "element while parsing %r attribute of "
                                  "limit at index %d; ignoring..." %
                                  (attr, idx))
                    continue

                value[grandchild.get('key')] = subtype(grandchild.text)
        elif attr_type == bool:
            try:
                value = config.Config.to_bool(child.text)
            except ValueError:
                warnings.warn("Unrecognized boolean value %r while parsing "
                              "%r attribute of limit at index %d; "
                              "ignoring..." % (child.text, attr, idx))
                continue
        else:
            # Simple type conversion
            value = attr_type(child.text)

        # Save the attribute
        attrs[attr] = value

        # Remove from the required set
        required.discard(attr)

    # Did we get all required attributes?
    if required:
        raise TypeError("Missing required attributes %s" %
                        (', '.join(repr(a) for a in sorted(required))))

    # OK, instantiate and return the class
    return klass(db, **attrs)


def _setup_limits(conf_file, limits_file, do_reload=True,
                  dry_run=False, debug=False):
    """
    Set up or update limits in the Redis database.

    :param conf_file: Name of the configuration file, for connecting
                      to the Redis database.
    :param limits_file: Name of the XML file describing the limits to
                        configure.
    :param do_reload: Controls reloading behavior.  If True (the
                      default), a reload command is issued.  If False,
                      no reload command is issued.  String values
                      result in a reload command of the given load
                      type, and integer or float values result in a
                      reload command of type 'spread' with the given
                      spread interval.
    :param dry_run: If True, no changes are made to the database.
                    Implies debug=True.
    :param debug: If True, debugging messages are emitted while
                  loading the limits and updating the database.
    """

    # If dry_run is set, default debug to True
    if dry_run:
        debug = True

    # Connect to the database...
    conf = config.Config(conf_file=conf_file)
    db = conf.get_database()
    limits_key = conf['control'].get('limits_key', 'limits')
    control_channel = conf['control'].get('channel', 'control')

    # Parse the limits file
    limits_tree = etree.parse(limits_file)

    # Now, we parse the limits XML file
    lims = []
    for idx, lim in enumerate(limits_tree.getroot()):
        # Skip tags we don't recognize
        if lim.tag != 'limit':
            warnings.warn("Unrecognized tag %r in limits file at index %d" %
                          (lim.tag, idx))
            continue

        # Construct the limit and add it to the list of limits
        try:
            lims.append(parse_limit_node(db, idx, lim))
        except Exception as exc:
            warnings.warn("Couldn't understand limit at index %d: %s" %
                          (idx, exc))
            continue

    # Now that we have the limits, let's install them
    if debug:
        print >>sys.stderr, "Installing the following limits:"
        for lim in lims:
            print >>sys.stderr, "  %r" % lim
    if not dry_run:
        db.limit_update(limits_key, lims)

    # Were we requested to reload the limits?
    if do_reload is False:
        return

    # OK, figure out what kind of reload to do
    params = []
    if do_reload is True:
        # Nothing to do; use default semantics
        pass
    elif (isinstance(do_reload, (int, long, float)) or
          (isinstance(do_reload, basestring) and do_reload.isdigit())):
        params = ['spread', do_reload]
    else:
        params = [str(do_reload)]

    # Issue the reload command
    if debug:
        cmd = ['reload']
        cmd.extend(params)
        print >>sys.stderr, ("Issuing command: %s" %
                             ' '.join(str(c) for c in cmd))
    if not dry_run:
        db.command(control_channel, 'reload', *params)


def setup_limits():
    """
    Console script entry point for setting up limits from an XML file.
    """

    parser = argparse.ArgumentParser(
        description="Set up or update limits in the Redis database.",
        )

    parser.add_argument('config',
                        help="Name of the configuration file, for connecting "
                        "to the Redis database.")
    parser.add_argument('limits_file',
                        help="Name of the XML file describing the limits to "
                        "configure.")
    parser.add_argument('--debug', '-d',
                        dest='debug',
                        action='store_true',
                        default=False,
                        help="Run the tool in debug mode.")
    parser.add_argument('--dryrun', '--dry_run', '--dry-run', '-n',
                        dest='dry_run',
                        action='store_true',
                        default=False,
                        help="Perform a dry run; inhibits loading data into "
                        "the database.")
    parser.add_argument('--noreload', '-R',
                        dest='reload',
                        action='store_false',
                        default=True,
                        help="Inhibit issuing a reload command.")
    parser.add_argument('--reload-immediate', '-r',
                        dest='reload',
                        action='store_const',
                        const='immediate',
                        help="Cause all nodes to immediately reload the "
                        "limits configuration.")
    parser.add_argument('--reload-spread', '-s',
                        dest='reload',
                        metavar='SECS',
                        type=float,
                        action='store',
                        help="Cause all nodes to reload the limits "
                        "configuration over the specified number of seconds.")

    args = parser.parse_args()
    try:
        _setup_limits(args.config, args.limits_file, args.reload,
                      args.dry_run, args.debug)
    except Exception as exc:
        if args.debug:
            raise
        return str(exc)


def make_limit_node(root, limit):
    """
    Given a Limit object, generate an XML node.

    :param root: The root node of the XML tree being built.
    :param limit: The Limit object to serialize to XML.
    """

    # Build the base limit node
    limit_node = etree.SubElement(root, 'limit',
                                  {'class': limit._limit_full_name})

    # Walk through all the recognized attributes
    for attr in sorted(limit.attrs):
        desc = limit.attrs[attr]
        attr_type = desc.get('type', str)
        value = getattr(limit, attr)

        # Determine the default value, if we have one...
        if 'default' in desc:
            default = (desc['default']() if callable(desc['default']) else
                       desc['default'])

            # Skip attributes that have their default settings
            if value == default:
                continue

        # Set up the attr node
        attr_node = etree.SubElement(limit_node, 'attr', name=attr)

        # Treat lists and dicts specially
        if attr_type == list:
            for val in value:
                val_node = etree.SubElement(attr_node, 'value')
                val_node.text = str(val)
        elif attr_type == dict:
            for key, val in sorted(value.items(), key=lambda x: x[0]):
                val_node = etree.SubElement(attr_node, 'value', key=key)
                val_node.text = str(val)
        else:
            attr_node.text = str(value)


def _dump_limits(conf_file, limits_file, debug=False):
    """
    Dump the current limits from the Redis database.

    :param conf_file: Name of the configuration file, for connecting
                      to the Redis database.
    :param limits_file: Name of the XML file that the limits will be
                        dumped to.
    :param debug: If True, debugging messages are emitted while
                  dumping the limits.
    """

    # Connect to the database...
    conf = config.Config(conf_file=conf_file)
    db = conf.get_database()
    limits_key = conf['control'].get('limits_key', 'limits')

    # Now, grab all the limits
    lims = [limits.Limit.hydrate(db, msgpack.loads(lim))
            for lim in db.zrange(limits_key, 0, -1)]

    # Build up the limits tree
    root = etree.Element('limits')
    limit_tree = etree.ElementTree(root)
    for idx, lim in enumerate(lims):
        if debug:
            print >>sys.stderr, "Dumping limit index %d: %r" % (idx, lim)
        make_limit_node(root, lim)

    # Write out the limits file
    if debug:
        print >>sys.stderr, "Dumping limits to file %r" % limits_file
    limit_tree.write(limits_file, xml_declaration=True, encoding='UTF-8',
                     pretty_print=True)


def dump_limits():
    """
    Console script entry point for dumping limits to an XML file.
    """

    parser = argparse.ArgumentParser(
        description="Dump the current limits from the Redis database.",
        )

    parser.add_argument('config',
                        help="Name of the configuration file, for connecting "
                        "to the Redis database.")
    parser.add_argument('limits_file',
                        help="Name of the XML file that the limits will be "
                        "dumped to.")
    parser.add_argument('--debug', '-d',
                        dest='debug',
                        action='store_true',
                        default=False,
                        help="Run the tool in debug mode.")

    args = parser.parse_args()
    try:
        _dump_limits(args.config, args.limits_file, args.debug)
    except Exception as exc:
        if args.debug:
            raise
        return str(exc)


def _remote_daemon(conf_file):
    """
    Run the external control daemon as configured.

    :param conf_file: Name of the configuration file.
    """

    eventlet.monkey_patch()
    conf = config.Config(conf_file=conf_file)
    daemon = remote.RemoteControlDaemon(None, conf)
    daemon.serve()


def remote_daemon():
    """
    Console script entry point for running the external control
    daemon.
    """

    parser = argparse.ArgumentParser(
        description="Run the external control daemon.",
        )

    parser.add_argument('config',
                        help="Name of the configuration file.")
    parser.add_argument('--log-config', '-l',
                        dest='logging',
                        action='store',
                        default=None,
                        help="Specify a logging configuration file.")
    parser.add_argument('--debug', '-d',
                        dest='debug',
                        action='store_true',
                        default=False,
                        help="Run the tool in debug mode.")

    args = parser.parse_args()

    # Set up logging
    if args.logging:
        logging.config.fileConfig(args.logging)
    else:
        logging.basicConfig()

    try:
        _remote_daemon(args.config)
    except Exception as exc:
        if args.debug:
            raise
        return str(exc)
