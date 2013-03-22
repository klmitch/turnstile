# Copyright 2013 Rackspace
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
import time
import traceback
import uuid

import msgpack

from turnstile import control
from turnstile import database
from turnstile import limits
from turnstile import remote
from turnstile import utils


LOG = logging.getLogger('turnstile')


def version_greater(minimum, version):
    """
    Compare two version strings.

    :param minimum: The minimum valid version.
    :param version: The version to compare to.

    :returns: True if version is greater than minimum, False
              otherwise.
    """

    # Chop up the version strings
    minimum = [int(i) for i in minimum.split('.')]
    version = [int(i) for i in version.split('.')]

    # Compare the versions element by element
    for mini, vers in zip(minimum, version):
        if vers < mini:
            # If it's less than, we definitely don't match
            return False
        elif vers > mini:
            # If it's greater than, we definitely match
            return True

        # OK, the elements are equal; loop around and check out the
        # next element

    # All elements are equal
    return True


def get_int(config, key, default):
    """
    A helper to retrieve an integer value from a given dictionary
    containing string values.  If the requested value is not present
    in the dictionary, or if it cannot be converted to an integer, a
    default value will be returned instead.

    :param config: The dictionary containing the desired value.
    :param key: The dictionary key for the desired value.
    :param default: The default value to return, if the key isn't set
                    in the dictionary, or if the value set isn't a
                    legal integer value.

    :returns: The desired integer value.
    """

    try:
        return int(config[key])
    except (KeyError, ValueError):
        return default


class GetBucketKey(object):
    """
    Bucket keys to be compacted are placed on a sorted set.  The
    compactor needs to atomically pop one bucket key off the set.
    This can be done with a lock--entailing the use of a lock key and
    various timeout mechanisms--or by evaluating a Lua script, which
    is probably the best way.  Unfortunately, Lua scripts are not
    supported prior to client version 2.7.0 or server version 2.6.0.
    This class provides an abstraction around these two methods,
    simplifying compactor().
    """

    @classmethod
    def factory(cls, config, db):
        """
        Given a configuration and database, select and return an
        appropriate instance of a subclass of GetBucketKey.  This will
        ensure that both client and server support are available for
        the Lua script feature of Redis, and if not, a lock will be
        used.

        :param config: A dictionary of compactor options.
        :param db: A database handle for the Redis database.

        :returns: An instance of a subclass of GetBucketKey, dependent
                  on the support for the Lua script feature of Redis.
        """

        # Make sure that the client supports register_script()
        if not hasattr(db, 'register_script'):
            LOG.debug("Redis client does not support register_script()")
            return GetBucketKeyByLock(config, db)

        # OK, the client supports register_script(); what about the
        # server?
        info = db.info()
        if version_greater('2.6', info['redis_version']):
            LOG.debug("Redis server supports register_script()")
            return GetBucketKeyByScript(config, db)

        # OK, use our fallback...
        LOG.debug("Redis server does not support register_script()")
        return GetBucketKeyByLock(config, db)

    def __init__(self, config, db):
        """
        Initialize a GetBucketKey instance.

        :param config: A dictionary of compactor options.
        :param db: A database handle for the Redis database.
        """

        self.db = db
        self.key = config.get('compactor_key', 'compactor')
        self.max_age = get_int(config, 'max_age', 600)
        self.min_age = get_int(config, 'min_age', 30)
        self.idle_sleep = get_int(config, 'sleep', 5)

    def __call__(self):
        """
        Retrieve the next bucket key to compact.  If no buckets are
        available for compacting, sleeps for a given period of time
        and tries again.

        :returns: The bucket key to compact.
        """

        while True:
            now = time.time()

            # Drop all items older than max_age; they're no longer
            # quiesced, since the compactor logic will cause new
            # summarize records to be generated.  No lock is needed...
            self.db.zrembyscore(self.key, 0, now - self.max_age)

            # Get an item and return it
            item = self.get(now)
            if item:
                LOG.debug("Next bucket to compact: %s" % item)
                return item

            # If we didn't get one, idle
            LOG.debug("No buckets to compact; sleeping for %s seconds" %
                      self.idle_sleep)
            time.sleep(self.idle_sleep)

    def get(self, now):
        """
        Get a bucket key to compact.  If none are available, returns
        None.

        :param now: The current time, as a float.  Used to ensure the
                    bucket key has been aged sufficiently to be
                    quiescent.

        :returns: A bucket key ready for compaction, or None if no
                  bucket keys are available or none have aged
                  sufficiently.
        """

        raise NotImplementedError()  # Pragma: nocover


class GetBucketKeyByLock(GetBucketKey):
    """
    Retrieve a bucket key to compact using a lock.
    """

    def __init__(self, config, db):
        """
        Initialize a GetBucketKeyByLock instance.

        :param config: A dictionary of compactor options.
        :param db: A database handle for the Redis database.
        """

        super(GetBucketKeyByLock, self).__init__(config, db)

        lock_key = config.get('compactor_lock', 'compactor_lock')
        timeout = get_int(config, 'compactor_timeout', 30)
        self.lock = db.lock(lock_key, timeout=timeout)

        LOG.debug("Using GetBucketKeyByLock as bucket key getter")

    def get(self, now):
        """
        Get a bucket key to compact.  If none are available, returns
        None.  This uses a configured lock to ensure that the bucket
        key is popped off the sorted set in an atomic fashion.

        :param now: The current time, as a float.  Used to ensure the
                    bucket key has been aged sufficiently to be
                    quiescent.

        :returns: A bucket key ready for compaction, or None if no
                  bucket keys are available or none have aged
                  sufficiently.
        """

        with self.lock:
            items = self.db.zrangebyscore(self.key, 0, now - self.min_age,
                                          start=0, num=1)
            # Did we get any items?
            if not items:
                return None

            # Drop the item we got
            item = items[0]
            self.db.zrem(item)

            return item


class GetBucketKeyByScript(GetBucketKey):
    """
    Retrieve a bucket key to compact using a Lua script.
    """

    def __init__(self, config, db):
        """
        Initialize a GetBucketKeyByScript instance.

        :param config: A dictionary of compactor options.
        :param db: A database handle for the Redis database.
        """

        super(GetBucketKeyByScript, self).__init__(config, db)

        self.script = db.register_script("""
local res
res = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'limit', 0, 1)
if #res > 0 then
    redis.call('zrem', res[1])
end
return res
""")

        LOG.debug("Using GetBucketKeyByScript as bucket key getter")

    def get(self, now):
        """
        Get a bucket key to compact.  If none are available, returns
        None.  This uses a Lua script to ensure that the bucket key is
        popped off the sorted set in an atomic fashion.

        :param now: The current time, as a float.  Used to ensure the
                    bucket key has been aged sufficiently to be
                    quiescent.

        :returns: A bucket key ready for compaction, or None if no
                  bucket keys are available or none have aged
                  sufficiently.
        """

        items = self.script(keys=[self.key], args=[now - self.min_age])
        return items[0] if items else None


class LimitContainer(object):
    """
    Contains a mapping of available limits.  To compact a bucket, the
    bucket needs to be loaded; this needs to be done by reference to
    the limit class, as the limit class specifies the bucket class to
    use and performs the appropriate processing of update records in
    the bucket list.

    Much of the code here is actually copied from the
    TurnstileMiddleware, suggesting that further abstraction is
    necessary.
    """

    def __init__(self, conf, db):
        """
        Initialize a LimitContainer.  This sets up an appropriate
        control daemon, as well as providing a container for the
        limits themselves.

        :param conf: A turnstile.config.Config instance containing the
                     configuration for the ControlDaemon.
        :param db: A database handle for the Redis database.
        """

        self.conf = conf
        self.db = db
        self.limits = []
        self.limit_map = {}
        self.limit_sum = None

        # Initialize the control daemon
        if conf.to_bool(conf['control'].get('remote', 'no'), False):
            self.control_daemon = remote.RemoteControlDaemon(self, conf)
        else:
            self.control_daemon = control.ControlDaemon(self, conf)

        # Now start the control daemon
        self.control_daemon.start()

    def __getitem__(self, key):
        """
        Obtain the limit with the given UUID.  Ensures that the
        current limit list is loaded.

        :param key: The UUID of the desired limit.
        """

        self.recheck_limits()
        return self.limit_map[key]

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

            # Save the new data
            self.limits = lims
            self.limit_map = dict((lim.uuid, lim) for lim in lims)
            self.limit_sum = new_sum
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


def compact_bucket(db, buck_key, limit):
    """
    Perform the compaction operation.  This reads in the bucket
    information from the database, builds a compacted bucket record,
    inserts that record in the appropriate place in the database, then
    removes outdated updates.

    :param db: A database handle for the Redis database.
    :param buck_key: A turnstile.limits.BucketKey instance containing
                     the bucket key.
    :param limit: The turnstile.limits.Limit object corresponding to
                  the bucket.
    """

    # Suck in the bucket records and generate our bucket
    records = db.lrange(str(buck_key), 0, -1)
    loader = limits.BucketLoader(limit.bucket_class, db, limit,
                                 str(buck_key), records, stop_summarize=True)

    # We now have the bucket loaded in; generate a 'bucket' record
    buck_record = msgpack.dumps(dict(bucket=loader.bucket.dehydrate(),
                                     uuid=str(uuid.uuid4())))

    # Now we need to insert it into the record list
    result = db.linsert(str(buck_key), 'after', loader.last_summarize_rec,
                        buck_record)

    # Were we successful?
    if result < 0:
        # Insert failed; we'll try again when max_age is hit
        LOG.warning("Bucket compaction on %s failed; will retry" % buck_key)
        return

    # OK, we have confirmed that the compacted bucket record has been
    # inserted correctly; now all we need to do is trim off the
    # outdated update records
    db.ltrim(str(buck_key), loader.last_summarize_idx + 1, -1)


def compactor(conf):
    """
    The compactor daemon.  This fuction watches the sorted set
    containing bucket keys that need to be compacted, performing the
    necessary compaction.

    :param conf: A turnstile.config.Config instance containing the
                 configuration for the compactor daemon.  Note that a
                 ControlDaemon is also started, so appropriate
                 configuration for that must also be present, as must
                 appropriate Redis connection information.
    """

    # Get the database handle
    db = conf.get_database('compactor')

    # Get the limits container
    limit_map = LimitContainer(conf, db)

    # Get the compactor configuration
    config = conf['compactor']

    # Make sure compaction is enabled
    if get_int(config, 'max_updates', 0) <= 0:
        # We'll just warn about it, since they could be running
        # the compactor with a different configuration file
        LOG.warning("Compaction is not enabled.  Enable it by "
                    "setting a positive integer value for "
                    "'compactor.max_updates' in the configuration.")

    # Select the bucket key getter
    key_getter = GetBucketKey.factory(config, db)

    LOG.info("Compactor initialized")

    # Now enter our loop
    while True:
        # Get a bucket key to compact
        try:
            buck_key = limits.BucketKey.decode(key_getter())
        except ValueError as exc:
            # Warn about invalid bucket keys
            LOG.warning("Error interpreting bucket key: %s" % exc)
            continue

        # Ignore version 1 keys--they can't be compacted
        if buck_key.version < 2:
            continue

        # Get the corresponding limit class
        try:
            limit = limit_map[buck_key.uuid]
        except KeyError:
            # Warn about missing limits
            LOG.warning("Unable to compact bucket for limit %s" %
                        buck_key.uuid)
            continue

        LOG.debug("Compacting bucket %s" % buck_key)

        # OK, we now have the limit (which we really only need for
        # the bucket class); let's compact the bucket
        try:
            compact_bucket(db, buck_key, limit)
        except Exception:
            LOG.exception("Failed to compact bucket %s" % buck_key)
        else:
            LOG.debug("Finished compacting bucket %s" % buck_key)
