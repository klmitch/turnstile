import logging
import traceback

import eventlet
import msgpack
import redis
import routes

from turnstile import limits
from turnstile import utils


LOG = logging.getLogger('turnstile')


class TurnstileRedis(redis.StrictRedis):
    def safe_update(self, key, klass, proc, *args):
        """
        Safely creates or updates an object in the database.

        :param key: The key the object should be looked up under.
        :param klass: The Python class corresponding to the object.
        :param proc: A callable taking a single argument--the object being
                     updated.  The return value of this callable will
                     become the return value of the safe_update()
                     function.  Note that the callable may be called
                     multiple times.

        If the object does not currently exist in the database, its
        constructor will be called with the database as the first
        parameter, followed by the remaining positional arguments.  If the
        object does currently exist in the database, its hydrate() class
        method will be called, again with the database as the first
        parameter, followed by the remaining positional arguments,
        followed by a dictionary.

        Once the object is built, the processor function is called; its
        return value will be saved and the object will be serialized back
        into the database (the object must have a dehydrate() instance
        method taking no parameters and returning a dictionary).  If the
        object has an 'expire' attribute, the key will be set to expire at
        the time given by 'expire'.

        Note that if the key is updated before processing is complete, the
        object will be reloaded and the processor function called again.
        This ensures that the processing is atomic, but means that the
        processor function must have no side effects other than changes to
        the object it is passed.
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
                        obj = klass.hydrate(self, *args, msgpack.loads(raw))

                    # Start the transaction...
                    pipe.multi()

                    # Call the processor...
                    result = proc(obj)

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

        return result


def initialize():
    """
    Initialize a connection to the Redis database.
    """

    pass


class MapperDaemon(object):
    """
    A daemon thread which listens for a reload event and reloads the
    limit configuration from the database.
    """

    def __init__(self, db, middleware, config):
        # Save some relevant information
        self.db = db
        self.middleware = middleware
        self.config = config

        # Need a semaphore to cover reloads in action
        self.pending = eventlet.semaphore.Semaphore()

        # Spawn the listening thread
        self.listen_thread = eventlet.spawn_n(self.listen)

        # Now do the initial load
        self.reload()

    def listen(self):
        # Need a pub-sub object
        kwargs = {}
        if 'shard_hint' in self.config:
            kwargs['shard_hint'] = self.config['shard_hint']
        pubsub = self.db.pubsub(**kwargs)

        # Subscribe to the right channel...
        channel = self.config.get('reload_channel', 'reload')
        pubsub.subscribe(channel)

        # Now we listen...
        for msg in pubsub.listen():
            # Only interested in messages to our reload channel
            if (msg['type'] not in ('pmessage', 'message') or
                msg['channel'] != channel):
                continue

            # OK, trigger a reload
            eventlet.spawn_n(self.reload)

    def reload(self):
        # Acquire the pending semaphore.  If we fail, exit--someone
        # else is already doing the reload
        if not self.pending.acquire(False):
            return

        # Do the remaining steps in a try/finally block so we make
        # sure to release the semaphore
        try:
            # Load all the limits
            key = self.config.get('limits_key', 'limits')
            limits = [limits.Limit.hydrate(self, msgpack.loads(lim))
                      for lim in self.db.zrange(key, 0, -1)]

            # Build the routes mapper
            mapper = routes.Mapper()
            for lim in limits:
                lim.route(mapper)

            # Install it
            self.middleware.mapper = mapper
        except Exception:
            # Format an error message
            msg = traceback.format_exc()

            # Log an error
            LOG.error("While reloading limits: %s" % msg)

            # Get our error set and publish channel
            error_key = self.config.get('errors_key', 'errors')
            error_channel = self.config.get('errors_channel', 'errors')

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
