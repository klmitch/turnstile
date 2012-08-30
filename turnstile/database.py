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

import msgpack
import redis

from turnstile import limits
from turnstile import utils


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


def limits_hydrate(db, lims):
    """
    Helper function to hydrate a list of limits.

    :param db: A database handle.
    :param lims: A list of limit strings, as retrieved from the
                 database.
    """

    return [limits.Limit.hydrate(db, lim) for lim in lims]
