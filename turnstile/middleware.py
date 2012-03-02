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
import math

from turnstile import database
from turnstile import utils


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
        self.mapper = None

        # Split up the configuration into groups of related variables
        self.config = {
            None: {
                'status': '413 Request Entity Too Large',
                },
            }
        for key, value in local_conf.items():
            outer, _sep, inner = key.partition('.')

            # Deal with prefix-less keys
            if not inner:
                outer, inner = None, outer

            # Make sure we have a place to put them
            self.config.setdefault(outer, {})
            self.config[outer][inner] = value

        # Set up request preprocessors
        self.preprocessors = []
        for preproc in self.config[None].get('preprocess', '').split():
            # Allow ImportError to bubble up
            self.preprocessors.append(utils.import_class(preproc))

        # Next, let's configure redis
        redis_args = self.config.get('redis', {})
        self.db = database.initialize(redis_args)

        # And start up the control daemon
        control_args = self.config.get('control', {})
        self.control_daemon = database.ControlDaemon(self.db, self,
                                                     control_args)

    def __call__(self, environ, start_response):
        """
        Implements the processing of the turnstile middleware.  Walks
        the list of limit filters, invoking their filters, then
        returns an appropriate response for the limit filter returning
        the longest delay.  If no limit filter indicates that a delay
        is needed, the request is passed on to the application.
        """

        # Run the request preprocessors
        for preproc in self.preprocessors:
            # Preprocessors are expected to modify the environment;
            # they are helpers to set up variables expected by the
            # limit classes.
            preproc(self, environ)

        # Make configuration available to the limit classes as well
        environ['turnstile.config'] = self.config

        # Now, if we have a mapper, run through it
        if self.mapper:
            self.mapper.routematch(environ=environ)

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
        status = self.config[None]['status']

        # Set up the retry-after header...
        headers = HeadersDict([('Retry-After', "%d" % math.ceil(delay))])

        # Let format fiddle with the headers
        status, entity = limit.format(status, headers, environ, bucket,
                                      delay)

        # Return the response
        start_response(status, headers.items())
        return entity
