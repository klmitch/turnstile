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

import hashlib
import logging
from multiprocessing import managers
import random
import traceback
import warnings

import eventlet
import msgpack

from turnstile import limits
from turnstile import utils


LOG = logging.getLogger('turnstile')


class NoChangeException(Exception):
    """
    Indicates that there are no limit data changes to be applied.
    Raised by LimitData.get_limits().
    """

    pass


class LimitData(object):
    """
    Stores limit data.  Provides a common depot between the
    ControlDaemon and the middleware which contains the raw limit data
    (as msgpack'd strings).
    """

    def __init__(self):
        """
        Initialize the LimitData.  The limit data is initialized to
        the empty list.
        """

        # Build up a sum for the empty list
        chksum = hashlib.md5()
        chksum.update('')

        self.limit_data = []
        self.limit_sum = chksum.hexdigest()
        self.limit_lock = eventlet.semaphore.Semaphore()

    def set_limits(self, limits):
        """
        Set the limit data to the given list of limits.  Limits are
        specified as the raw msgpack string representing the limit.
        Computes the checksum of the limits; if the checksum is
        identical to the current one, no action is taken.
        """

        # First task, build the checksum of the new limits
        chksum = hashlib.md5()  # sufficient for our purposes
        for lim in limits:
            chksum.update(lim)
        new_sum = chksum.hexdigest()

        # Now install it
        with self.limit_lock:
            if self.limit_sum == new_sum:
                # No changes
                return
            self.limit_data = limits[:]
            self.limit_sum = new_sum

    def get_limits(self, db, limit_sum=None):
        """
        Gets the current limit data if it is different from the data
        indicated by limit_sum.  The db argument is used for hydrating
        the limit objects.  Raises a NoChangeException if the
        limit_sum represents no change, otherwise returns a tuple
        consisting of the current limit_sum and a list of Limit
        objects.
        """

        with self.limit_lock:
            # Any changes?
            if limit_sum and self.limit_sum == limit_sum:
                raise NoChangeException()

            # Allow returning just the list of strings
            if not db:
                return (self.limit_sum, self.limit_data)

            # Return a tuple of the limits and limit sum
            lims = [limits.Limit.hydrate(db, msgpack.loads(lim))
                    for lim in self.limit_data]
            return (self.limit_sum, lims)


class ControlDaemon(object):
    """
    A daemon thread which listens for control messages and can reload
    the limit configuration from the database.
    """

    _commands = {}

    @classmethod
    def _register(cls, name, func):
        """
        Register func as a recognized control command with the given
        name.
        """

        cls._commands[name] = func

    def __init__(self, middleware, conf):
        """
        Initialize the ControlDaemon.  Starts the listening thread and
        triggers an immediate reload.
        """

        # Save some relevant information
        self._db = None
        self.middleware = middleware
        self.config = conf
        self.limits = LimitData()

        # Need a semaphore to cover reloads in action
        self.pending = eventlet.semaphore.Semaphore()

        # Initialize the listening thread
        self.listen_thread = None

    def start(self):
        """
        Starts the ControlDaemon by launching the listening thread and
        triggering the initial limits load.
        """

        # Spawn the listening thread
        self.listen_thread = eventlet.spawn_n(self.listen)

        # Now do the initial load
        self.reload()

    def listen(self):
        """
        Listen for incoming control messages.

        If the 'redis.shard_hint' configuration is set, its value will
        be passed to the pubsub() method when setting up the
        subscription.  The control channel to subscribe to is
        specified by the 'redis.control_channel' configuration
        ('control' by default).
        """

        # Use a specific database handle, with override.  This allows
        # the long-lived listen thread to be configured to use a
        # different database or different database options.
        db = self.config.get_database('control')

        # Need a pub-sub object
        kwargs = {}
        if 'shard_hint' in self.config['control']:
            kwargs['shard_hint'] = self.config['control']['shard_hint']
        pubsub = db.pubsub(**kwargs)

        # Subscribe to the right channel(s)...
        channel = self.config['control'].get('channel', 'control')
        pubsub.subscribe(channel)

        # Now we listen...
        for msg in pubsub.listen():
            # Only interested in messages to our reload channel
            if (msg['type'] in ('pmessage', 'message') and
                msg['channel'] == channel):
                # Figure out what kind of message this is
                command, _sep, args = msg['data'].partition(':')

                # We must have some command...
                if not command:
                    continue

                # Don't do anything with internal commands
                if command[0] == '_':
                    LOG.error("Cannot call internal command %r" % command)
                    continue

                # Don't do anything with missing commands
                try:
                    func = self._commands[command]
                except KeyError:
                    LOG.error("No such command %r" % command)
                    continue

                # Execute the desired command
                arglist = args.split(':')
                try:
                    func(self, *arglist)
                except Exception:
                    LOG.exception("Failed to execute command %r arguments %r" %
                                  (command, arglist))
                    continue

    def get_limits(self):
        """
        Retrieve the LimitData object the middleware will use for
        getting the limits.  This is broken out into a function so
        that it can be overridden in multi-process configurations to
        return a LimitData subclass which will query the master
        LimitData in the ControlDaemon process.
        """

        return self.limits

    def reload(self):
        """
        Reloads the limits configuration from the database.

        If an error occurs loading the configuration, an error-level
        log message will be emitted.  Additionally, the error message
        will be added to the set specified by the 'redis.errors_key'
        configuration ('errors' by default) and sent to the publishing
        channel specified by the 'redis.errors_channel' configuration
        ('errors' by default).
        """

        # Acquire the pending semaphore.  If we fail, exit--someone
        # else is already doing the reload
        if not self.pending.acquire(False):
            return

        # Do the remaining steps in a try/finally block so we make
        # sure to release the semaphore
        control_args = self.config['control']
        try:
            # Load all the limits
            key = control_args.get('limits_key', 'limits')
            self.limits.set_limits(self.db.zrange(key, 0, -1))
        except Exception:
            # Log an error
            LOG.exception("Could not load limits")

            # Get our error set and publish channel
            error_key = control_args.get('errors_key', 'errors')
            error_channel = control_args.get('errors_channel', 'errors')

            # Get an informative message
            msg = "Failed to load limits: " + traceback.format_exc()

            # Store the message into the error set.  We use a set here
            # because it's likely that more than one node will
            # generate the same message if there is an error, and this
            # avoids an explosion in the size of the set.
            with utils.ignore_except():
                self.db.sadd(error_key, msg)

            # Publish the message to a channel
            with utils.ignore_except():
                self.db.publish(error_channel, msg)
        finally:
            self.pending.release()

    @property
    def db(self):
        """
        Obtain a handle for the database.  This allows lazy
        initialization of the database handle.
        """

        # Initialize the database handle from the middleware's copy of
        # it
        if not self._db:
            self._db = self.middleware.db

        return self._db


class RemoteLimitData(LimitData):
    """
    Provides remote access to limit data stored in another process.
    This interacts with the multiprocessing module's Manager support
    to provide seamless access to the limit data collected by (and
    stored in) the MultiControlDaemon process.
    """

    def __init__(self, manager):
        """
        Initialize RemoteLimitData.  Stores a reference to the Manager
        object.
        """

        self._manager = manager

    @property
    def limit_data(self):
        """
        Read-only access to the limit_data field stored on the remote
        Manager object.
        """

        return self._manager.limit_data()._getvalue()

    @property
    def limit_sum(self):
        """
        Read-only access to the limit_sum field stored on the remote
        Manager object.
        """

        return self._manager.limit_sum()._getvalue()

    @property
    def limit_lock(self):
        """
        Read-only access to the limit_lock field stored on the remote
        Manager object.
        """

        return self._manager.limit_lock()

    def set_limits(self, limits):
        """
        Remote limit data is treated as read-only (with external
        update).
        """

        raise ValueError("Cannot set remote limit data")


class AcquirerProxy(managers.BaseProxy):
    """
    Copied from multiprocessing.  Allows the limit_lock to be used to
    acquire and release locks, complete with context manager-style
    access.
    """

    _exposed_ = ('acquire', 'release')

    def acquire(self, blocking=True):
        """
        Proxy the acquire() method of the semaphore.
        """

        return self._callmethod('acquire', (blocking,))

    def release(self):
        """
        Proxy the release() method of the semaphore.
        """

        return self._callmethod('release')

    def __enter__(self):
        """
        Proxy the __enter__() method of the semaphore.
        """

        return self._callmethod('acquire')

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Proxy the __exit__() method of the semaphore.
        """

        return self._callmethod('release')


class MultiControlDaemon(ControlDaemon):
    """
    A daemon process which listens for control messages and can reload
    the limit configuration from the database.  Based on the
    ControlDaemon, but starts a multiprocessing Manager process to
    enable access to the limit data from multiple processes.
    """

    def __init__(self, middleware, conf):
        """
        Initialize the MultiControlDaemon.  Starts the Manager and the
        listening thread and triggers an immediate reload.
        """

        # Grab required configuration values
        required = set(['multi.host', 'multi.port', 'multi.authkey'])
        values = {}
        for conf_key in list(required):
            key = conf_key[6:]
            try:
                if key == 'port':
                    values[key] = int(conf['control'][conf_key])
                else:
                    values[key] = conf['control'][conf_key]
            except KeyError:
                warnings.warn("Missing value for configuration key "
                              "'control.%s'" % conf_key)
            except ValueError:
                warnings.warn("Invalid port value %r" %
                              conf['control'][conf_key])
            else:
                required.discard(conf_key)

        # Error out if we're missing something critical
        if required:
            raise ValueError("Missing required configuration for "
                             "MultiControlDaemon.  Missing or invalid "
                             "configuration keys: %s" %
                             ', '.join(['control.%s' % k for k in
                                        sorted(required)]))

        super(MultiControlDaemon, self).__init__(middleware, conf)

        # Build a LimitManager
        class LimitManager(managers.BaseManager):
            pass

        LimitManager.register('limit_data', lambda: self.limits.limit_data)
        LimitManager.register('limit_sum', lambda: self.limits.limit_sum)
        LimitManager.register('limit_lock', lambda: self.limits.limit_lock,
                              AcquirerProxy)

        # Prepare the manager and the remote limit data
        self.manager = LimitManager((values['host'], values['port']),
                                    values['authkey'])
        self.remote = RemoteLimitData(self.manager)

    def get_limits(self):
        """
        Retrieve the LimitData object the middleware will use for
        getting the limits.  This implementation returns a
        RemoteLimitData instance that can access the LimitData stored
        in the MultiControlDaemon process.
        """

        return self.remote

    def start(self):
        """
        Starts the MultiControlDaemon by connecting to the running
        control daemon process.  If the control daemon process is not
        currently running, a socket error will be raised.
        """

        self.manager.connect()

    def serve(self):
        """
        Starts the MultiControlDaemon process.  Forks a thread for
        listening to the Redis database, then initializes and starts
        the Manager.
        """

        # Start the listening thread and load the limits
        super(MultiControlDaemon, self).start()

        # Start the manager in this thread
        self.manager.get_server().serve_forever()

    @property
    def db(self):
        """
        Obtain a handle for the database.  This allows lazy
        initialization of the database handle.
        """

        # Initialize the database handle; we're running in a separate
        # process, so we need to get_database() ourself
        if not self._db:
            self._db = self.config.get_database()

        return self._db


def register(name, func=None):
    """
    Function or decorator which registers a given function as a
    recognized control command.
    """

    def decorator(func):
        # Perform the registration
        ControlDaemon._register(name, func)
        return func

    # If func was given, call the decorator, otherwise, return the
    # decorator
    if func:
        return decorator(func)
    else:
        return decorator


@register('ping')
def ping(daemon, channel, data=None):
    """
    Process the 'ping' control message.

    :param daemon: The control daemon; used to get at the
                   configuration and the database.
    :param channel: The publish channel to which to send the
                    response.
    :param data: Optional extra data.  Will be returned as the
                 second argument of the response.

    Responds to the named channel with a command of 'pong' and
    with the node_name (if configured) and provided data as
    arguments.
    """

    if not channel:
        # No place to reply to
        return

    # Get our configured node name
    node_name = daemon.config['control'].get('node_name')

    # Format the response
    reply = ['pong']
    if node_name or data:
        reply.append(node_name or '')
    if data:
        reply.append(data)

    # And send it
    with utils.ignore_except():
        daemon.db.publish(channel, ':'.join(reply))


@register('reload')
def reload(daemon, load_type=None, spread=None):
    """
    Process the 'reload' control message.

    :param daemon: The control daemon; used to get at the
                   configuration and call the actual reload.
    :param load_type: Optional type of reload.  If given as
                      'immediate', reload is triggered
                      immediately.  If given as 'spread', reload
                      is triggered after a random period of time
                      in the interval (0.0, spread).  Otherwise,
                      reload will be as configured.
    :param spread: Optional argument for 'spread' load_type.  Must
                   be a float giving the maximum length of the
                   interval, in seconds, over which the reload
                   should be scheduled.  If not provided, falls
                   back to configuration.

    If a recognized load_type is not given, or is given as
    'spread' but the spread parameter is not a valid float, the
    configuration will be checked for the 'redis.reload_spread'
    value.  If that is a valid value, the reload will be randomly
    scheduled for some time within the interval (0.0,
    redis.reload_spread).
    """

    # Figure out what type of reload this needs to be
    if load_type == 'immediate':
        spread = None
    elif load_type == 'spread':
        try:
            spread = float(spread)
        except (TypeError, ValueError):
            # Not a valid float; use the configured spread value
            load_type = None
    else:
        load_type = None

    if load_type is None:
        # Use configured set-up; see if we have a spread
        # configured
        try:
            spread = float(daemon.config['control']['reload_spread'])
        except (TypeError, ValueError, KeyError):
            # No valid configuration
            spread = None

    if spread:
        # Apply a randomization to spread the load around
        eventlet.spawn_after(random.random() * spread, daemon.reload)
    else:
        # Spawn in immediate mode
        eventlet.spawn_n(daemon.reload)
