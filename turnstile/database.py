import msgpack
import redis


def safe_update(db, key, klass, proc, *args):
    """
    Safely creates or updates an object in the database.

    :param db: The database the object is in.
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
    with db.pipeline() as pipe:
        while True:
            try:
                # Watch for changes to the key
                pipe.watch(key)

                # Look up or create the object
                raw = pipe.get(key)
                if raw is None:
                    obj = klass(db, *args)
                else:
                    obj = klass.hydrate(db, *args, msgpack.loads(raw))

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
