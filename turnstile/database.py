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
import random
import traceback

import eventlet
import msgpack
import redis
import routes

from turnstile import limits
from turnstile import utils


LOG = logging.getLogger('turnstile')


class TurnstileRedis(redis.StrictRedis):
    def safe_update(self, key, klass, update, *args):
        """
        Safely creates or updates an object in the database.

        :param key: The key the object should be looked up under.
        :param klass: The Python class corresponding to the object.
        :param update: A callable taking a single argument--the object
                       being updated.  The return value of this
                       callable will become the return value of the
                       safe_update() function.  Note that the callable
                       may be called multiple times for a single
                       update.

        If the object does not currently exist in the database, its
        constructor will be called with the database as the first
        parameter, followed by the remaining positional arguments.  If the
        object does currently exist in the database, its hydrate() class
        method will be called, again with the database as the first
        parameter, followed by the remaining positional arguments,
        followed by a dictionary.

        Once the object is built, the update function is called; its
        return value will be saved and the object will be serialized back
        into the database (the object must have a dehydrate() instance
        method taking no parameters and returning a dictionary).  If the
        object has an 'expire' attribute, the key will be set to expire at
        the time given by 'expire'.

        Note that if the key is updated before processing is complete,
        the object will be reloaded and the update function called
        again.  This ensures that the update is atomic, but means that
        the update function must have no side effects other than
        changes to the object it is passed.
        """

        # Do this in a transaction
        with self.pipeline() as pipe:
            while True:
                try:
                    # Watch for changes to the key
                    pipe.watch(key)

                    # Look up or create the object
                    raw = pipe.get(key)
                    if raw is None:
                        obj = klass(self, *args)
                    else:
                        obj = klass.hydrate(self, msgpack.loads(raw), *args)

                    # Start the transaction...
                    pipe.multi()

                    # Call the processor...
                    result = update(obj)

                    # Save the object back to the database
                    pipe.set(key, msgpack.dumps(obj.dehydrate()))

                    # Set its expiration time
                    try:
                        pipe.expireat(key, obj.expire)
                    except AttributeError:
                        # No expiration time available
                        pass

                    # Execute the transaction
                    pipe.execute()
                except redis.WatchError:
                    # Try again...
                    continue
                else:
                    # We're all done!
                    break

        return result

    def limit_update(self, key, limits):
        """
        Safely updates the list of limits in the database.

        :param key: The key the limits are stored under.
        :param limits: A list or sequence of limit objects, each
                       understanding the dehydrate() method.

        The limits list currently in the database will be atomically
        changed to match the new list.  This is done using the
        pipeline() method.
        """

        # Start by dehydrating all the limits
        desired = [msgpack.dumps(l.dehydrate()) for l in limits]
        desired_set = set(desired)

        # Now, let's update the limits
        with self.pipeline() as pipe:
            while True:
                try:
                    # Watch for changes to the key
                    pipe.watch(key)

                    # Look up the existing limits
                    existing = set(pipe.zrange(key, 0, -1))

                    # Start the transaction...
                    pipe.multi()

                    # Remove limits we no longer have
                    for lim in existing - desired_set:
                        pipe.zrem(key, lim)

                    # Update or add all our desired limits
                    for idx, lim in enumerate(desired):
                        pipe.zadd(key, (idx + 1) * 10, lim)

                    # Execute the transaction
                    pipe.execute()
                except redis.WatchError:
                    # Try again...
                    continue
                else:
                    # We're all done!
                    break

    def command(self, channel, command, *args):
        """
        Utility method to issue a command to all Turnstile instances.

        :param channel: The control channel all Turnstile instances
                        are listening on.
        :param command: The command, as plain text.  Currently, only
                        'reload' and 'ping' are recognized.

        All remaining arguments are treated as arguments for the
        command; they will be stringified and sent along with the
        command to the control channel.  Note that ':' is an illegal
        character in arguments, but no warnings will be issued if it
        is used.
        """

        # Build the command we're sending
        cmd = [command]
        cmd.extend(str(a) for a in args)

        # Send it out
        self.publish(channel, ':'.join(cmd))


def initialize(config):
    """
    Initialize a connection to the Redis database.
    """

    # Extract relevant connection information from the configuration
    kwargs = {}
    for cfg_var, type_ in [('host', str), ('port', int), ('db', int),
                           ('password', str), ('socket_timeout', int),
                           ('unix_socket_path', str)]:
        if cfg_var in config:
            kwargs[cfg_var] = type_(config[cfg_var])

    # Make sure we have at a minimum the hostname
    if 'host' not in kwargs and 'unix_socket_path' not in kwargs:
        raise redis.ConnectionError("No host specified for redis database")

    # Look up the connection pool configuration
    cpool_class = None
    cpool = {}
    for key, value in config.items():
        if key.startswith('connection_pool.'):
            _dummy, _sep, varname = key.partition('.')
            if varname == 'connection_class':
                cpool[varname] = utils.import_class(value)
            elif varname == 'max_connections':
                cpool[varname] = int(value)
            elif varname == 'parser_class':
                cpool[varname] = utils.import_class(value)
            else:
                cpool[varname] = value
    if cpool:
        cpool_class = redis.ConnectionPool

    # Use custom connection pool class if requested...
    if 'connection_pool' in config:
        cpool_class = utils.import_class(config['connection_pool'])

    # If we're using a connection pool, we'll need to pass the keyword
    # arguments to that instead of to redis
    if cpool_class:
        cpool.update(kwargs)

        # Use a custom connection class?
        if 'connection_class' not in cpool:
            if 'unix_socket_path' in cpool:
                if 'host' in cpool:
                    del cpool['host']
                if 'port' in cpool:
                    del cpool['port']

                cpool['path'] = cpool['unix_socket_path']
                del cpool['unix_socket_path']

                cpool['connection_class'] = redis.UnixDomainSocketConnection
            else:
                cpool['connection_class'] = redis.Connection

        # Build the connection pool to use and set up to pass it into
        # the redis constructor...
        kwargs = dict(connection_pool=cpool_class(**cpool))

    # Build and return the database
    return TurnstileRedis(**kwargs)


class ControlDaemon(object):
    """
    A daemon thread which listens for control messages and can reload
    the limit configuration from the database.
    """

    def __init__(self, db, middleware, config):
        """
        Initialize the ControlDaemon.  Starts the listening thread and
        triggers an immediate reload.
        """

        # Save some relevant information
        self._db = db
        self._middleware = middleware
        self._config = config

        # Need a semaphore to cover reloads in action
        self._pending = eventlet.semaphore.Semaphore()

        # Start the daemon
        self._start()

    def _start(self):
        """
        Starts the ControlDaemon by launching the listening thread and
        triggering the initial limits load.  Broken out of __init__()
        for testing.
        """

        # Spawn the listening thread
        self._listen_thread = eventlet.spawn_n(self._listen)

        # Now do the initial load
        self._reload()

    def _listen(self):
        """
        Listen for incoming control messages.

        If the 'redis.shard_hint' configuration is set, its value will
        be passed to the pubsub() method when setting up the
        subscription.  The control channel to subscribe to is
        specified by the 'redis.control_channel' configuration
        ('control' by default).
        """

        # Need a pub-sub object
        kwargs = {}
        if 'shard_hint' in self._config:
            kwargs['shard_hint'] = self._config['shard_hint']
        pubsub = self._db.pubsub(**kwargs)

        # Subscribe to the right channel(s)...
        channel = self._config.get('channel', 'control')
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
                    LOG.error("Cannot call internal method %r" % command)
                    continue

                # Don't do anything with missing commands
                try:
                    method = getattr(self, command)
                except AttributeError:
                    LOG.error("No such command %r" % command)
                    continue

                # Don't do anything with non-callables
                if not callable(method):
                    LOG.error("Command %r is not callable" % command)
                    continue

                # Execute the desired command
                arglist = args.split(':')
                try:
                    method(*arglist)
                except Exception:
                    LOG.exception("Failed to execute command %r arguments %r" %
                                  (command, arglist))
                    continue

    def _reload(self):
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
        if not self._pending.acquire(False):
            return

        # Do the remaining steps in a try/finally block so we make
        # sure to release the semaphore
        try:
            # Load all the limits
            key = self._config.get('limits_key', 'limits')
            lims = [limits.Limit.hydrate(self._db, msgpack.loads(lim))
                    for lim in self._db.zrange(key, 0, -1)]

            # Build the routes mapper
            mapper = routes.Mapper(register=False)
            for lim in lims:
                lim._route(mapper)

            # Install it
            self._middleware.limits = lims
            self._middleware.mapper = mapper
        except Exception:
            # Log an error
            LOG.exception("Could not load limits")

            # Get our error set and publish channel
            error_key = self._config.get('errors_key', 'errors')
            error_channel = self._config.get('errors_channel', 'errors')

            # Get an informative message
            msg = "Failed to load limits: " + traceback.format_exc()

            # Store the message into the error set.  We use a set here
            # because it's likely that more than one node will
            # generate the same message if there is an error, and this
            # avoids an explosion in the size of the set.
            with utils.ignore_except():
                self._db.sadd(error_key, msg)

            # Publish the message to a channel
            with utils.ignore_except():
                self._db.publish(error_channel, msg)
        finally:
            self._pending.release()

    def ping(self, channel, data=None):
        """
        Process the 'ping' control message.

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
        node_name = self._config.get('node_name')

        # Format the response
        reply = ['pong']
        if node_name or data:
            reply.append(node_name or '')
        if data:
            reply.append(data)

        # And send it
        with utils.ignore_except():
            self._db.publish(channel, ':'.join(reply))

    def reload(self, load_type=None, spread=None):
        """
        Process the 'reload' control message.

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
                spread = float(self._config['reload_spread'])
            except (TypeError, ValueError, KeyError):
                # No valid configuration
                spread = None

        if spread:
            # Apply a randomization to spread the load around
            eventlet.spawn_after(random.random() * spread, self._reload)
        else:
            # Spawn in immediate mode
            eventlet.spawn_n(self._reload)
