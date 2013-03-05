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


def initialize(config):
    """
    Initialize a connection to the Redis database.
    """

    # Determine the client class to use
    if 'redis_client' in config:
        client = utils.find_entrypoint('turnstile.redis_client',
                                       config['redis_client'], required=True)
    else:
        client = redis.StrictRedis

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
                cpool[varname] = utils.find_entrypoint(
                    'turnstile.connection_class', value, required=True)
            elif varname == 'max_connections':
                cpool[varname] = int(value)
            elif varname == 'parser_class':
                cpool[varname] = utils.find_entrypoint(
                    'turnstile.parser_class', value, required=True)
            else:
                cpool[varname] = value
    if cpool:
        cpool_class = redis.ConnectionPool

    # Use custom connection pool class if requested...
    if 'connection_pool' in config:
        cpool_class = utils.find_entrypoint('turnstile.connection_pool',
                                            config['connection_pool'],
                                            required=True)

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
    return client(**kwargs)


def limits_hydrate(db, lims):
    """
    Helper function to hydrate a list of limits.

    :param db: A database handle.
    :param lims: A list of limit strings, as retrieved from the
                 database.
    """

    return [limits.Limit.hydrate(db, lim) for lim in lims]


def limit_update(db, key, limits):
    """
    Safely updates the list of limits in the database.

    :param db: The database handle.
    :param key: The key the limits are stored under.
    :param limits: A list or sequence of limit objects, each
                   understanding the dehydrate() method.

    The limits list currently in the database will be atomically
    changed to match the new list.  This is done using the pipeline()
    method.
    """

    # Start by dehydrating all the limits
    desired = [msgpack.dumps(l.dehydrate()) for l in limits]
    desired_set = set(desired)

    # Now, let's update the limits
    with db.pipeline() as pipe:
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


def command(db, channel, command, *args):
    """
    Utility function to issue a command to all Turnstile instances.

    :param db: The database handle.
    :param channel: The control channel all Turnstile instances are
                    listening on.
    :param command: The command, as plain text.  Currently, only
                    'reload' and 'ping' are recognized.

    All remaining arguments are treated as arguments for the command;
    they will be stringified and sent along with the command to the
    control channel.  Note that ':' is an illegal character in
    arguments, but no warnings will be issued if it is used.
    """

    # Build the command we're sending
    cmd = [command]
    cmd.extend(str(a) for a in args)

    # Send it out
    db.publish(channel, ':'.join(cmd))
