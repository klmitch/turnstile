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

import functools
import inspect
import logging
import logging.config
import pprint
import sys
import textwrap
import time
import uuid
import warnings

import argparse
import eventlet
from lxml import etree
import msgpack

from turnstile import compactor
from turnstile import config
from turnstile import database
from turnstile import limits
from turnstile import remote
from turnstile import utils


class ScriptAdaptor(object):
    """
    Special wrapper for a console script entrypoint.  This allows
    command line arguments to be specified using decorators.  The
    underlying function may be called directly, if that is desired.  A
    short description of the function, derived from the function
    docstring, is available in the 'description' attribute.
    """

    @classmethod
    def _wrap(cls, func):
        """
        Ensures that the function is wrapped in a ScriptAdaptor
        object.  If it is not, a new ScriptAdaptor will be returned.
        If it is, the ScriptAdaptor is returned.

        :param func: The function to be wrapped.
        """

        if isinstance(func, cls):
            return func
        return functools.update_wrapper(cls(func), func)

    def __init__(self, func):
        """
        Initialize a ScriptAdaptor.

        :param func: The underlying function.
        """

        self._func = func
        self._preprocess = []
        self._postprocess = []
        self._arguments = []

        # Extract the description of the script
        desc = []
        for line in textwrap.dedent(func.__doc__ or '').strip().split('\n'):
            # Clean up the line...
            line = line.strip()

            # We only want the first paragraph
            if not line:
                break

            desc.append(line)

        # Save the description
        self.description = ' '.join(desc)

    def __call__(self, *args, **kwargs):
        """
        Call the function directly.  All arguments are passed to the
        function, and the function's return value is returned.
        """

        return self._func(*args, **kwargs)

    def _add_argument(self, args, kwargs):
        """
        Add an argument specification.

        :param args: Positional arguments for the underlying
                     ArgumentParser.add_argument() method.
        :param kwargs: Keyword arguments for the underlying
                       ArgumentParser.add_argument() method.
        """

        self._arguments.insert(0, (args, kwargs))

    def _add_preprocessor(self, func):
        """
        Add a preprocessor.  Preprocessors run after parsing the
        arguments and before the underlying function is executed, and
        may be used to set up such things as logging.

        :param func: The function to be added as a preprocessor.
        """

        self._preprocess.insert(0, func)

    def _add_postprocessor(self, func):
        """
        Add a postprocessor.  Postprocessors run after calling the
        underlying function, and may be used to report return results.

        :param func: The function to be added as a preprocessor.
        """

        self._postprocess.insert(0, func)

    def setup_args(self, parser):
        """
        Set up an argparse.ArgumentParser object by adding all the
        arguments taken by the function.
        """

        # Add all the arguments to the argument parser
        for args, kwargs in self._arguments:
            parser.add_argument(*args, **kwargs)

    def get_kwargs(self, args):
        """
        Given a Namespace object drawn from argparse, determines the
        keyword arguments to pass to the underlying function.  Note
        that, if the underlying function accepts all keyword
        arguments, the dictionary returned will contain the entire
        contents of the Namespace object.  Also note that an
        AttributeError will be raised if any argument required by the
        function is not set in the Namespace object.

        :param args: A Namespace object from argparse.
        """

        # Now we need to figure out which arguments the final function
        # actually needs
        kwargs = {}
        argspec = inspect.getargspec(self._func)
        required = set(argspec.args[:-len(argspec.defaults)]
                       if argspec.defaults else argspec.args)
        for arg_name in argspec.args:
            try:
                kwargs[arg_name] = getattr(args, arg_name)
            except AttributeError:
                if arg_name in required:
                    # If this happens, that's a programming failure
                    raise

        # If the function accepts any keyword argument, add whatever
        # remains
        if argspec.keywords:
            for key, value in args.__dict__.items():
                if key in kwargs:
                    # Already handled
                    continue
                kwargs[key] = value

        return kwargs

    def safe_call(self, kwargs, args=None):
        """
        Call the underlying function safely, given a set of keyword
        arguments.  If successful, the function return value (likely
        None) will be returned.  If the underlying function raises an
        exception, the return value will be the exception message,
        unless an argparse Namespace object defining a 'debug'
        attribute of True is provided; in this case, the exception
        will be re-raised.

        :param kwargs: A dictionary of keyword arguments to pass to
                       the underlying function.
        :param args: If provided, this should be a Namespace object
                     with a 'debug' attribute set to a boolean value.

        :returns: The function return value, or the string value of
                  the exception raised by the function.
        """

        # Now let's call the function
        try:
            return self._func(**kwargs)
        except Exception as exc:
            if args and getattr(args, 'debug', False):
                raise
            return str(exc)

    def console(self):
        """
        Call the function as a console script.  Command line arguments
        are parsed, preprocessors are called, then the function is
        called.  If a 'debug' attribute is set by the command line
        arguments, and it is True, any exception raised by the
        underlying function will be reraised; otherwise, the return
        value will be either the return value of the function or the
        text contents of the exception.
        """

        # First, let's parse the arguments
        parser = argparse.ArgumentParser(description=self.description)
        self.setup_args(parser)
        args = parser.parse_args()

        # Next, let's run the preprocessors in order
        for proc in self._preprocess:
            try:
                proc(args)
            except Exception as exc:
                if getattr(args, 'debug', False):
                    raise
                return str(exc)

        # Finally, safely call the underlying function
        result = self.safe_call(self.get_kwargs(args), args)

        # Now, run the postprocessors in order
        for proc in self._postprocess:
            result = proc(args, result)

        return result


def add_argument(*args, **kwargs):
    """
    Define an argument for the function when running in console script
    mode.  The positional and keyword arguments are the same as for
    ArgumentParser.add_argument().
    """

    def decorator(func):
        func = ScriptAdaptor._wrap(func)
        func._add_argument(args, kwargs)
        return func
    return decorator


def add_preprocessor(preproc):
    """
    Define a preprocessor to run after the arguments are parsed and
    before the function is executed, when running in console script
    mode.

    :param preproc: The callable, which will be passed the Namespace
                    object generated by argparse.
    """

    def decorator(func):
        func = ScriptAdaptor._wrap(func)
        func._add_preprocessor(preproc)
        return func
    return decorator


def add_postprocessor(postproc):
    """
    Define a postprocessor to run after the function is executed, when
    running in console script mode.

    :param postproc: The callable, which will be passed the Namespace
                     object generated by argparse and the return
                     result of the function.  The return result of the
                     callable will be used as the final return result
                     (or as the result fed into the next
                     postprocessor).
    """

    def decorator(func):
        func = ScriptAdaptor._wrap(func)
        func._add_postprocessor(postproc)
        return func
    return decorator


def _setup_logging(args):
    """
    Set up logging for the script, based on the configuration
    specified by the 'logging' attribute of the command line
    arguments.

    :param args: A Namespace object containing a 'logging' attribute
                 specifying the name of a logging configuration file
                 to use.  If not present or not given, a basic logging
                 configuration will be set.
    """

    log_conf = getattr(args, 'logging', None)
    if log_conf:
        logging.config.fileConfig(log_conf)
    else:
        logging.basicConfig()


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
    klass = utils.find_entrypoint('turnstile.limit', limit.get('class'),
                                  required=True)

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
            try:
                for j, grandchild in enumerate(child):
                    if grandchild.tag != 'value':
                        warnings.warn("Unrecognized element %r while parsing "
                                      "%r attribute of limit at index %d; "
                                      "ignoring element..." %
                                      (grandchild.tag, attr, idx))
                        continue

                    value.append(subtype(grandchild.text))
            except ValueError:
                warnings.warn("Invalid value %r while parsing element %d "
                              "of %r attribute of limit at index %d; "
                              "ignoring attribute..." %
                              (grandchild.text, j, attr, idx))
                continue
        elif attr_type == dict:
            # Dicts are expressed as child elements, with the tags
            # identifying the attribute name
            subtype = desc.get('subtype', str)
            value = {}
            for grandchild in child:
                if grandchild.tag != 'value':
                    warnings.warn("Unrecognized element %r while parsing "
                                  "%r attribute of limit at index %d; "
                                  "ignoring element..." %
                                  (grandchild.tag, attr, idx))
                    continue
                elif 'key' not in grandchild.attrib:
                    warnings.warn("Missing 'key' attribute of 'value' "
                                  "element while parsing %r attribute of "
                                  "limit at index %d; ignoring element..." %
                                  (attr, idx))
                    continue

                try:
                    value[grandchild.get('key')] = subtype(grandchild.text)
                except ValueError:
                    warnings.warn("Invalid value %r while parsing %r element "
                                  "of %r attribute of limit at index %d; "
                                  "ignoring element..." %
                                  (grandchild.text, grandchild.get('key'),
                                   attr, idx))
                    continue
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
            try:
                value = attr_type(child.text)
            except ValueError:
                warnings.warn("Invalid value %r while parsing %r attribute "
                              "of limit at index %d; ignoring..." %
                              (child.text, attr, idx))
                continue

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


@add_argument('conf_file',
              metavar='config',
              help="Name of the configuration file, for connecting "
              "to the Redis database.")
@add_argument('limits_file',
              help="Name of the XML file describing the limits to "
              "configure.")
@add_argument('--debug', '-d',
              dest='debug',
              action='store_true',
              default=False,
              help="Run the tool in debug mode.")
@add_argument('--dryrun', '--dry_run', '--dry-run', '-n',
              dest='dry_run',
              action='store_true',
              default=False,
              help="Perform a dry run; inhibits loading data into "
              "the database.")
@add_argument('--noreload', '-R',
              dest='do_reload',
              action='store_false',
              default=True,
              help="Inhibit issuing a reload command.")
@add_argument('--reload-immediate', '-r',
              dest='do_reload',
              action='store_const',
              const='immediate',
              help="Cause all nodes to immediately reload the "
              "limits configuration.")
@add_argument('--reload-spread', '-s',
              dest='do_reload',
              metavar='SECS',
              type=float,
              action='store',
              help="Cause all nodes to reload the limits "
              "configuration over the specified number of seconds.")
def setup_limits(conf_file, limits_file, do_reload=True,
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
        database.limit_update(db, limits_key, lims)

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
        database.command(db, control_channel, 'reload', *params)


# For backwards compatibility
_setup_limits = setup_limits


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


@add_argument('conf_file',
              metavar='config',
              help="Name of the configuration file, for connecting "
              "to the Redis database.")
@add_argument('limits_file',
              help="Name of the XML file that the limits will be "
              "dumped to.")
@add_argument('--debug', '-d',
              dest='debug',
              action='store_true',
              default=False,
              help="Run the tool in debug mode.")
def dump_limits(conf_file, limits_file, debug=False):
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


# For backwards compatibility
_dump_limits = dump_limits


@add_argument('conf_file',
              metavar='config',
              help="Name of the configuration file.")
@add_argument('--log-config', '-l',
              dest='logging',
              action='store',
              default=None,
              help="Specify a logging configuration file.")
@add_argument('--debug', '-d',
              dest='debug',
              action='store_true',
              default=False,
              help="Run the tool in debug mode.")
@add_preprocessor(_setup_logging)
def remote_daemon(conf_file):
    """
    Run the external control daemon.

    :param conf_file: Name of the configuration file.
    """

    eventlet.monkey_patch()
    conf = config.Config(conf_file=conf_file)
    daemon = remote.RemoteControlDaemon(None, conf)
    daemon.serve()


# For backwards compatibility
_remote_daemon = remote_daemon


@add_argument('conf_file',
              metavar='config',
              help="Name of the configuration file.")
@add_argument('command',
              help="The command to execute.  Note that 'ping' is handled "
              "specially; in particular, the --listen parameter is implied.")
@add_argument('arguments',
              nargs='*',
              help="The arguments to pass for the command.  Note that the "
              "colon character (':') cannot be used.")
@add_argument('--listen', '-l',
              dest='channel',
              action='store',
              default=None,
              help="A channel to listen on for the command responses.  Use "
              "C-c (or your systems keyboard interrupt sequence) to stop "
              "waiting for responses.")
@add_argument('--debug', '-d',
              dest='debug',
              action='store_true',
              default=False,
              help="Run the tool in debug mode.")
def turnstile_command(conf_file, command, arguments=[], channel=None,
                      debug=False):
    """
    Issue a command to all running control daemons.

    :param conf_file: Name of the configuration file.
    :param command: The command to execute.  Note that 'ping' is
                    handled specially; in particular, the "channel"
                    parameter is implied.  (A random value will be
                    used for the channel to listen on.)
    :param arguments: A list of arguments for the command.  Note that
                      the colon character (':') cannot be used.
    :param channel: If not None, specifies the name of a message
                    channel to listen for responses on.  Will wait
                    indefinitely; to terminate the listening loop, use
                    the keyboard interrupt sequence.
    :param debug: If True, debugging messages are emitted while
                  sending the command.
    """

    # Connect to the database...
    conf = config.Config(conf_file=conf_file)
    db = conf.get_database()
    control_channel = conf['control'].get('channel', 'control')

    # Now, set up the command
    command = command.lower()
    ts_conv = False
    if command == 'ping':
        # We handle 'ping' specially; first, figure out the channel
        if arguments:
            channel = arguments[0]
        else:
            channel = str(uuid.uuid4())
            arguments = [channel]

        # Next, add on a timestamp
        if len(arguments) < 2:
            arguments.append(time.time())
            ts_conv = True

        # Limit the argument list length
        arguments = arguments[:2]

    # OK, the command is all set up.  Let us now send the command...
    if debug:
        cmd = [command] + arguments
        print >>sys.stderr, ("Issuing command: %s" %
                             ' '.join(cmd))
    database.command(db, control_channel, command, *arguments)

    # Were we asked to listen on a channel?
    if not channel:
        return

    # OK, let's subscribe to the channel...
    pubsub = db.pubsub()
    pubsub.subscribe(channel)

    # Now we listen...
    try:
        count = 0
        for msg in pubsub.listen():
            # Make sure the message is one we're interested in
            if debug:
                formatted = pprint.pformat(msg)
                print >>sys.stderr, "Received message: %s" % formatted
            if (msg['type'] not in ('pmessage', 'message') or
                    msg['channel'] != channel):
                continue

            count += 1

            # Figure out the response
            response = msg['data'].split(':')

            # If this is a 'pong' and ts_conv is true, add an RTT to
            # the response
            if ts_conv and response[0] == 'pong':
                try:
                    rtt = (time.time() - float(response[2])) * 100
                    response.append('(RTT %.2fms)' % rtt)
                except Exception:
                    # IndexError or ValueError, probably; ignore it
                    pass

            # Print out the response
            print "Response % 5d: %s" % (count, ' '.join(response))
    except KeyboardInterrupt:
        # We want to break out of the loop, but not return any error
        # to the caller...
        pass


@add_argument('conf_file',
              metavar='config',
              help="Name of the configuration file.")
@add_argument('--log-config', '-l',
              dest='logging',
              action='store',
              default=None,
              help="Specify a logging configuration file.")
@add_argument('--debug', '-d',
              dest='debug',
              action='store_true',
              default=False,
              help="Run the tool in debug mode.")
@add_preprocessor(_setup_logging)
def compactor_daemon(conf_file):
    """
    Run the compactor daemon.

    :param conf_file: Name of the configuration file.
    """

    eventlet.monkey_patch()
    conf = config.Config(conf_file=conf_file)
    compactor.compactor(conf)
