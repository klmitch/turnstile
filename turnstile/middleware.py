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

import collections
import logging
import math
import traceback

import eventlet
import routes

from turnstile import config
from turnstile import control
from turnstile import database
from turnstile import remote
from turnstile import utils


LOG = logging.getLogger('turnstile')


class HeadersDict(collections.MutableMapping):
    """
    A dictionary class for headers.  All keys are mapped to lowercase.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize HeadersDict.  Uses update() to process additional
        arguments.
        """

        self.headers = {}
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        """
        Retrieve an item.
        """

        return self.headers[key.lower()]

    def __setitem__(self, key, value):
        """
        Set an item.
        """

        self.headers[key.lower()] = value

    def __delitem__(self, key):
        """
        Delete an item.
        """

        del self.headers[key.lower()]

    def __contains__(self, key):
        """
        Test if the headers dictionary contains a given header.
        """

        return key.lower() in self.headers

    def __iter__(self):
        """
        Iterate through the headers dictionary.
        """

        return iter(self.headers)

    def __len__(self):
        """
        Retrieve the length of the headers dictionary.
        """

        return len(self.headers)

    def iterkeys(self):
        """
        Iterate through header names.
        """

        return self.headers.iterkeys()

    def iteritems(self):
        """
        Iterate through header items.
        """

        return self.headers.iteritems()

    def itervalues(self):
        """
        Iterate through header values.
        """

        return self.headers.itervalues()

    def keys(self):
        """
        Return a list of header names.
        """

        return self.headers.keys()

    def items(self):
        """
        Return a list of header items.
        """

        return self.headers.items()

    def values(self):
        """
        Return a list of header values.
        """

        return self.headers.values()


def turnstile_filter(global_conf, **local_conf):
    """
    Factory function for turnstile.

    Returns a function which, when passed the application, returns an
    instance of the TurnstileMiddleware.
    """

    # Select the appropriate middleware class to return
    klass = TurnstileMiddleware
    if 'turnstile' in local_conf:
        klass = utils.import_class(local_conf['turnstile'])

    def wrapper(app):
        return klass(app, local_conf)

    return wrapper


class TurnstileMiddleware(object):
    """
    Turnstile Middleware.

    Instances of this class are WSGI middleware which perform the
    desired rate limiting.
    """

    def __init__(self, app, local_conf):
        """
        Initialize the turnstile middleware.  Saves the configuration
        and sets up the list of preprocessors, connects to the
        database, and initiates the control daemon thread.
        """

        # Save the application
        self.app = app
        self.limits = []
        self.limit_sum = None
        self.mapper = None
        self.mapper_lock = eventlet.semaphore.Semaphore()

        # Save the configuration
        self.conf = config.Config(conf_dict=local_conf)

        # We will lazy-load the database
        self._db = None

        # Set up request preprocessors
        self.preprocessors = []
        for preproc in self.conf.get('preprocess', '').split():
            # Allow ImportError to bubble up
            self.preprocessors.append(utils.import_class(preproc))

        # Initialize the control daemon
        if config.Config.to_bool(self.conf['control'].get('remote', 'no'),
                                 False):
            self.control_daemon = remote.RemoteControlDaemon(self, self.conf)
        else:
            self.control_daemon = control.ControlDaemon(self, self.conf)

        # Now start the control daemon
        self.control_daemon.start()

        # Emit a log message to indicate that we're running
        LOG.info("Turnstile middleware initialized")

    def recheck_limits(self):
        """
        Re-check that the cached limits are the current limits.
        """

        limit_data = self.control_daemon.get_limits()

        try:
            # Get the new checksum and list of limits
            new_sum, new_limits = limit_data.get_limits(self.limit_sum)

            # Convert the limits list into a list of objects
            lims = database.limits_hydrate(self.db, new_limits)

            # Build a new mapper
            mapper = routes.Mapper(register=False)
            for lim in lims:
                lim._route(mapper)

            # Save the new data
            self.limits = lims
            self.limit_sum = new_sum
            self.mapper = mapper
        except control.NoChangeException:
            # No changes to process; just keep going...
            return
        except Exception:
            # Log an error
            LOG.exception("Could not load limits")

            # Get our error set and publish channel
            control_args = self.conf['control']
            error_key = control_args.get('errors_key', 'errors')
            error_channel = control_args.get('errors_channel', 'errors')

            # Get an informative message
            msg = "Failed to load limits: " + traceback.format_exc()

            # Store the message into the error set.  We use a set
            # here because it's likely that more than one node
            # will generate the same message if there is an error,
            # and this avoids an explosion in the size of the set.
            with utils.ignore_except():
                self.db.sadd(error_key, msg)

            # Publish the message to a channel
            with utils.ignore_except():
                self.db.publish(error_channel, msg)

    def __call__(self, environ, start_response):
        """
        Implements the processing of the turnstile middleware.  Walks
        the list of limit filters, invoking their filters, then
        returns an appropriate response for the limit filter returning
        the longest delay.  If no limit filter indicates that a delay
        is needed, the request is passed on to the application.
        """

        with self.mapper_lock:
            # Check for updates to the limits
            self.recheck_limits()

            # Grab the current mapper
            mapper = self.mapper

            # Run the request preprocessors; some may want to refer to
            # the limit data, so protect this in the mapper_lock
            for preproc in self.preprocessors:
                # Preprocessors are expected to modify the environment;
                # they are helpers to set up variables expected by the
                # limit classes.
                preproc(self, environ)

        # Make configuration available to the limit classes as well
        environ['turnstile.config'] = self.config  # compat
        environ['turnstile.conf'] = self.conf

        # Now, if we have a mapper, run through it
        if mapper:
            mapper.routematch(environ=environ)

        # If there were any delays, deal with them
        if 'turnstile.delay' in environ and environ['turnstile.delay']:
            # Find the longest delay
            delay, limit, bucket = sorted(environ['turnstile.delay'],
                                          key=lambda x: x[0])[-1]

            return self.format_delay(delay, limit, bucket,
                                     environ, start_response)

        return self.app(environ, start_response)

    def format_delay(self, delay, limit, bucket, environ, start_response):
        """
        Formats the over-limit response for the request.  May be
        overridden in subclasses to allow alternate responses.
        """

        # Set up the default status
        status = self.conf.status

        # Set up the retry-after header...
        headers = HeadersDict([('Retry-After', "%d" % math.ceil(delay))])

        # Let format fiddle with the headers
        status, entity = limit.format(status, headers, environ, bucket,
                                      delay)

        # Return the response
        start_response(status, headers.items())
        return entity

    @property
    def config(self):
        """
        Obtain the configuration as a multi-level dictionary.
        Provided for backwards compatibility.
        """

        return self.conf._config

    @property
    def db(self):
        """
        Obtain a handle for the database.  This allows lazy
        initialization of the database handle.
        """

        # Initialize the database handle
        if not self._db:
            self._db = self.conf.get_database()

        return self._db
