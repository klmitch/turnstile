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

import json
import math
import re
import time
import uuid

import metatools
import msgpack

from turnstile import utils


class DeferLimit(Exception):
    """Exception raised if limit should not be considered."""

    pass


def _make_units(*units):
    """
    Units map helper.  Each argument is treated as a unit
    descriptor--a tuple where the first element specifies a numerical
    value, and the second element consists of a sequence of names for
    that value.  Returns a dictionary where each name is mapped to the
    numerical value, and the numerical value is mapped to the first
    name.
    """

    units_map = {}
    for value, names in units:
        units_map[value] = names[0]
        for name in names:
            units_map[name] = value

    return units_map


class TimeUnit(object):
    """
    A custom type for a time unit.  Initialized with the time unit in
    either string or integer form.  To obtain the unit name, use
    str(); to obtain the integer value, use int().
    """

    _units_map = _make_units(
        (1, ('second', 'seconds', 'secs', 'sec', 's')),
        (60, ('minute', 'minutes', 'mins', 'min', 'm')),
        (60 * 60, ('hour', 'hours', 'hrs', 'hr', 'h')),
        (60 * 60 * 24, ('day', 'days', 'd')),
    )

    def __init__(self, value):
        """
        Initialize a TimeUnit object.

        :param value: The value.  This may be a recognized unit name,
                      or an integer value in either string or integer
                      format.  A ValueError will be raised if the
                      value cannot be interpreted.
        """

        if isinstance(value, (int, long)):
            # Numbers map to numbers
            pass
        elif not isinstance(value, basestring):
            # Prohibit anything other than a string
            raise ValueError('unit must be a string')
        elif value.isdigit():
            # If it's all digits, it maps to a number
            value = int(value)
        else:
            # Look it up in the units map...
            try:
                value = self._units_map[value.lower()]
            except KeyError:
                raise ValueError('unknown unit %r' % value)

        # Prohibit negative numbers
        if value <= 0:
            raise ValueError('unit must be a positive integer, not %r' % value)

        self.value = value

    def __str__(self):
        """Return the string representation of the time unit."""

        return self._units_map.get(self.value, str(self.value))

    def __int__(self):
        """Return the integer value of the time unit."""

        return self.value


class BucketKey(object):
    """
    Represent a bucket key.  This class provides functionality to
    serialize parameters into a bucket key, and to deserialize a
    bucket key into the original parameters used to build it.  The key
    version, associated limit UUID, and the request parameters are all
    encoded into a bucket key.

    Instances of this class have three attributes:

      uuid
        The UUID of the corresponding limit.

      params
        A dictionary of the request parameters corresponding to the
        bucket.

      version
        An integer specifying the version of the bucket key.  At
        present, only two versions (1 and 2) are available.  Version 1
        buckets are stored as a msgpack'd dictionary in a string field
        in the Redis database, while version 2 buckets are stored as a
        list of msgpack'd dictionaries.

    To obtain the string key, use str() on instances of this class.
    """

    # Map prefixes to versions and vice versa
    _prefix_to_version = dict(bucket=1, bucket_v2=2)
    _version_to_prefix = dict((v, k) for k, v in _prefix_to_version.items())

    # Regular expressions for encoding and decoding
    _ENC_RE = re.compile('[/%]')
    _DEC_RE = re.compile('%([a-fA-F0-9]{2})')

    @classmethod
    def _encode(cls, value):
        """Encode the given value, taking care of '%' and '/'."""

        value = json.dumps(value)
        return cls._ENC_RE.sub(lambda x: '%%%2x' % ord(x.group(0)), value)

    @classmethod
    def _decode(cls, value):
        """Decode the given value, reverting '%'-encoded groups."""

        value = cls._DEC_RE.sub(lambda x: '%c' % int(x.group(1), 16), value)
        return json.loads(value)

    def __init__(self, uuid, params, version=2):
        """
        Initialize a BucketKey.

        :param uuid: The UUID of the limit the bucket corresponds to.
        :param params: A dictionary of the request parameters
                       corresponding to the bucket.
        :param version: The version of the bucket.  Optional; defaults
                        to 2.  Most callers should use the default.
        """

        # Make sure we can represent the version of the bucket key
        if version not in self._version_to_prefix:
            raise ValueError("Unknown bucket key version %r" % version)

        # Save the parameters
        self.uuid = uuid
        self.params = params
        self.version = version

        # Cache the string version of the key for effiency
        self._cache = None

    def __str__(self):
        """
        Returns the string form of the bucket key.
        """

        # If not cached, serialize the key
        if self._cache is None:
            parts = ['%s:%s' % (self._version_to_prefix[self.version],
                                self.uuid)]
            parts.extend('%s=%s' % (k, self._encode(v)) for k, v in
                         sorted(self.params.items(), key=lambda x: x[0]))
            self._cache = '/'.join(parts)

        return self._cache

    @classmethod
    def decode(cls, key):
        """
        Decode a bucket key into a BucketKey instance.

        :param key: The string form of a bucket key.

        :returns: A suitable instance of BucketKey corresponding to
                  the passed-in key.
        """

        # Determine bucket key version
        prefix, sep, param_str = key.partition(':')
        if sep != ':' or prefix not in cls._prefix_to_version:
            raise ValueError("%r is not a bucket key" % key)
        version = cls._prefix_to_version[prefix]

        # Take the parameters apart...
        parts = param_str.split('/')
        uuid = parts.pop(0)
        params = {}
        for part in parts:
            name, sep, value = part.partition('=')

            # Make sure it's well-formed
            if sep != '=':
                raise ValueError("Cannot interpret parameter expression %r" %
                                 part)

            params[name] = cls._decode(value)

        # Return a BucketKey
        return cls(uuid, params, version=version)


class BucketLoader(object):
    """
    Load a bucket from its list representation.

    The list representation includes several different record types,
    representing a base bucket, an update to a bucket, and a
    summarize-in-progress record.  This class will not only load a
    bucket from that list representation, it will also accumulate all
    auxiliary information needed for the algorithms below.

    This is implemented as a class due to the complexity of the
    information that the algorithm needs to return.  The return
    information is represented by the following instance attributes:

      bucket
        The actual bucket, with all applicable updates applied.

      updates
        A count of the updates applied to the bucket.  Used by the
        utility routine need_summary() to determine if the bucket
        representation should be summarized (a compacting algorithm
        used to limit database record growth).

      delay
        The return result of the delay() bucket method from the last
        update record processed.  This will be the delay to apply to a
        rate-limited request, or None if the request is not
        rate-limited.

      summarized
        A boolean value indicating the presence of "summarize" records
        in the bucket representation.  This is used to further inform
        need_summary() as to whether a "summarize" record should be
        added.  (The compacting algorithm is robust against multiple
        "summarize" records being present, as long as no more
        "summarize" records can be added while the bucket is being
        compacted.  This is accomplished by ensuring that a
        "summarize" request quiesces for a given period of time before
        it is processed.)

      last_summarize_idx
        If not None, this is the integer, 0-based index of the last
        "summarize" record in the bucket representation.  The
        compacting algorithm assembles a final bucket up to this last
        "summarize" record, then inserts the full bucket after it; it
        will then trim off all previous records, including this last
        "summarize" record, to finish compacting the entry.  The index
        of the last "summarize" record is used to ensure the insert
        and the trim cover the appropriate records.

      last_summarize_rec
        If not None, this is the raw, packed string representing the
        last "summarize" record in the bucket representation.  The
        compacting algorithm assembles a final bucket up to this last
        "summarize" record, then inserts the full bucket after it; it
        will then trim off all previous records, including this last
        "summarize" record, to finish compacting the entry.  The
        record text is needed to determine the location to insert the
        compacted bucket record.

      last_summarize_ts
        If not None, this is the timestamp on the last "summarize"
        record in the bucket representation.  This is used to ensure
        that the bucket will be eventually summarized, even if a
        summarize request was lost by the compactor.
    """

    def __init__(self, bucket_class, db, limit, key, records,
                 stop_uuid=None, stop_summarize=False):
        """
        Initialize a BucketLoader.  Generates the bucket from the list
        of records.

        :param bucket_class: The class of the bucket.
        :param db: The database handle for the bucket.
        :param limit: The limit object asociated with the bucket.
        :param key: The database key identifying the bucket record
                    list.
        :param records: A list of msgpack'd strings containing the
                        change records for the bucket.
        :param stop_uuid: The UUID of the last record (typically an
                          update record) which should be processed.
        :param stop_summarize: If True, indicates that processing
                               should be stopped once the _last_
                               summarize record is encountered.
        """

        # Initialize the loading algorithm
        self.bucket = None
        self.updates = 0
        self.delay = None
        self.summarized = False
        self.last_summarize_idx = None
        self.last_summarize_rec = None
        self.last_summarize_ts = None

        # Unpack the records
        unpacked = [msgpack.loads(rec) for rec in records]

        # If stop_summarize is set, we need to find the last summary
        # record
        if stop_summarize:
            for i, rec in enumerate(reversed(unpacked)):
                # If it's a summarize record, store the index and
                # timestamp of the record within the list
                if 'summarize' in rec:
                    self.summarized = True
                    self.last_summarize_ts = rec['summarize']
                    self.last_summarize_idx = len(records) - i - 1
                    self.last_summarize_rec = records[self.last_summarize_idx]
                    break

        # Now, build the bucket
        no_update = False
        for i, rec in enumerate(unpacked):
            # Break out if we hit the last summary record
            if (self.last_summarize_idx is not None and
                    i == self.last_summarize_idx):
                break

            # Update the bucket as appropriate
            if 'bucket' in rec:
                # If we hit no_update, we're done rendering, but still
                # need to look for 'summarize' records
                if no_update:
                    continue

                # We have an actual bucket record; render it
                self.bucket = bucket_class.hydrate(db, rec['bucket'],
                                                   limit, key)
            elif 'update' in rec:
                # If we hit no_update, we're done rendering, but still
                # need to look for 'summarize' records
                if no_update:
                    continue

                # We have an update record; first make sure we have a
                # bucket to update
                if self.bucket is None:
                    self.bucket = bucket_class(db, limit, key)

                # Now, update the bucket, saving the computed delay
                self.delay = self.bucket.delay(rec['update']['params'],
                                               rec['update']['time'])

                # Keep count of the number of updates, so we can
                # generate a summary if needed
                self.updates += 1
            elif 'summarize' in rec:
                # We have a summarize record; remember that one exists
                self.summarized = True

                # Look for the oldest summarize record and remember
                # its timestamp
                ts = rec['summarize']
                if (self.last_summarize_ts is None or
                        ts > self.last_summarize_ts):
                    self.last_summarize_ts = ts

            # If we hit the last record we're supposed to process,
            # stop
            if stop_uuid is not None and rec.get('uuid') == stop_uuid:
                # There may be 'summarize' records still to find, so
                # just suspend updates to the bucket but search the
                # rest of the record list
                no_update = True

        # Make sure we have a bucket at the end
        if self.bucket is None:
            self.bucket = bucket_class(db, limit, key)

    def need_summary(self, now, max_updates, max_age):
        """
        Helper method to determine if a "summarize" record should be
        added.

        :param now: The current time.
        :param max_updates: Maximum number of updates before a
                            summarize is required.
        :param max_age: Maximum age of the last summarize record.
                        This is used in the case where a summarize
                        request has been lost by the compactor.

        :returns: True if a "summarize" record should be added, False
                  otherwise.
        """

        # Handle the case where an old summarize record exists
        if self.summarized is True and self.last_summarize_ts + max_age <= now:
            return True

        return self.summarized is False and self.updates >= max_updates


class Bucket(object):
    """
    Represent a "bucket."  A bucket tracks the necessary values for
    application of the leaky bucket algorithm under the control of a
    limit specification.
    """

    attrs = set(['last', 'next', 'level'])
    eps = 0.1

    def __init__(self, db, limit, key, last=None, next=None, level=0.0):
        """
        Initialize a bucket.

        :param db: The database the bucket is in.
        :param limit: The limit associated with this bucket.
        :param key: The key under which this bucket should be stored.
        :param last: The timestamp of the last request.
        :param next: The timestamp of the next permissible request.
        :param level: The current water level in the bucket.
        """

        self.db = db
        self.limit = limit
        self.key = key
        self.last = last
        self.next = next
        self.level = level

    @classmethod
    def hydrate(cls, db, bucket, limit, key):
        """
        Given a key and a bucket dict, as generated by dehydrate(),
        generate an appropriate instance of Bucket.
        """

        return cls(db, limit, key, **bucket)

    def dehydrate(self):
        """Return a dict representing this bucket."""

        # Only concerned about very specific attributes
        result = {}
        for attr in self.attrs:
            result[attr] = getattr(self, attr)

        return result

    def delay(self, params, now=None):
        """Determine delay until next request."""

        if now is None:
            now = time.time()

        # Initialize last...
        if not self.last:
            self.last = now
        elif now < self.last:
            now = self.last

        # How much has leaked out?
        leaked = now - self.last

        # Update the last message time
        self.last = now

        # Update the water level
        self.level = max(self.level - leaked, 0)

        # Are we too full?
        difference = self.level + self.limit.cost - self.limit.unit_value
        if difference >= self.eps:
            self.next = now + difference
            return difference

        # OK, raise the water level and set next to an appropriate
        # value
        self.level += self.limit.cost
        self.next = now

        return None

    @property
    def messages(self):
        """Return remaining messages before limiting."""

        return int(math.floor(((self.limit.unit_value - self.level) /
                               self.limit.unit_value) * self.limit.value))

    @property
    def expire(self):
        """Return the estimated expiration time of this bucket."""

        # Round up and convert to an int
        return int(math.ceil(self.last + self.level))


class LimitMeta(metatools.MetaClass):
    """
    Metaclass for limits.
    """

    _registry = {}

    def __new__(mcs, name, bases, namespace):
        """
        Generate a new Limit class.  Adds the full class name to the
        namespace, for the benefit of dehydrate().  Also registers the
        class in the registry, for the benefit of hydrate().
        """

        # Build the full name
        full_name = '%s:%s' % (namespace['__module__'], name)

        # Add it to the namespace
        namespace['_limit_full_name'] = full_name

        # Set up attrs
        namespace.setdefault('attrs', {})
        for base in mcs.iter_bases(bases):
            mcs.inherit_dict(base, namespace, 'attrs')

        # Create the class
        cls = super(LimitMeta, mcs).__new__(mcs, name, bases, namespace)

        # Register the class
        if full_name not in mcs._registry:
            mcs._registry[full_name] = cls

        return cls


class Limit(object):
    """
    Basic limit.  This can be used as an absolute rate limit on a
    given endpoint or set of endpoints.  All other limit classes must
    subclass this class.
    """

    __metaclass__ = LimitMeta

    attrs = dict(
        uuid=dict(
            desc=('A UUID uniquely identifying the limit.  If not provided, '
                  'a new one will be generated when the limit is '
                  'instantiated.'),
            type=str,
            default=lambda: str(uuid.uuid4()),
        ),
        uri=dict(
            desc=('The URI the limit applies to.  This should be in a syntax '
                  'recognized by Routes, i.e., "/constant/{variable}".  Note '
                  'that this URI may be displayed to the user.  Required.'),
            type=str,
        ),
        value=dict(
            desc=('The permissible number of requests per unit time.  '
                  'Required.'),
            type=int,
        ),
        unit=dict(
            desc=('The unit of time over which the "value" is considered.  '
                  'This may be a string, such as "second", or an integer '
                  'number of seconds, expressed as a string.  Required.'),
            type=TimeUnit,
        ),
        verbs=dict(
            desc=('The HTTP verbs this limit should apply to.  Optional.  If '
                  'not provided, this limit matches any request to the URI; '
                  'otherwise, only the listed methods match.  Takes a list of '
                  'strings.'),
            type=list,
            subtype=str,
            default=lambda: [],  # Make sure we don't use the *same* list
            xform=lambda verbs: [v.upper() for v in verbs],
        ),
        requirements=dict(
            desc=('A mapping of variable names in the URI to regular '
                  'expressions; may be used to further restrict a given '
                  'variable to a particular string.  This could be used '
                  'to differentiate a request to "/resource/{id}" from '
                  'a request to "/resource/edit".  Optional.'),
            type=dict,
            subtype=str,
            default=lambda: {},  # Make sure we don't use the *same* dict
        ),
        queries=dict(
            desc=('A list of query arguments that must be present in the '
                  'request in order for this limit to apply.  Query argument '
                  'values are not automatically added to the list of '
                  'parameters used to construct the bucket key.'),
            type=list,
            subtype=str,
            default=lambda: [],  # Make sure we don't use the *same* list
        ),
        use=dict(
            desc=('A list of parameters derived from the URI which should be '
                  'used to construct the bucket key.  By default, no '
                  'parameters are used; this provides a way to list the '
                  'set of parameters to use.'),
            type=list,
            subtype=str,
            default=lambda: [],  # Make sure we don't use the *same* list
        ),
        continue_scan=dict(
            desc=('A boolean which signals whether to consider limits '
                  'following this one in the list.  If True (the '
                  'default), the remaining limits are scanned even if '
                  'this limit matches.  May be set to False to skip '
                  'remaining limits.'),
            type=bool,
            default=True,
        ),
    )

    bucket_class = Bucket

    def __init__(self, db, **kwargs):
        """
        Initialize a new limit.

        :param db: The database the limit object is in.

        For the permissible keyword arguments, see the `attrs`
        dictionary.
        """

        self.db = db

        # Save the various arguments
        missing = set()
        for attr, desc in self.attrs.items():
            # A value is provided
            if attr in kwargs:
                value = kwargs[attr]

                # Run the transformer, if one was specified
                if 'xform' in desc:
                    value = desc['xform'](value)
            elif 'default' in desc:
                # Use the default value; if it's callable, call it
                value = (desc['default']() if callable(desc['default']) else
                         desc['default'])
            else:
                # Missing attribute
                missing.add(attr)
                continue

            # Save the attribute value
            setattr(self, attr, value)

        # Did we get all the required attributes?
        if missing:
            raise TypeError("Missing required attributes: %s" %
                            ', '.join(sorted(missing)))

    def __repr__(self):
        """
        Return a representation of the limit.
        """

        base = [self._limit_full_name]
        for attr in sorted(self.attrs):
            desc = self.attrs[attr]
            attr_type = desc.get('type', str)

            # Treat lists and dicts specially
            if attr_type == list:
                sublist = [repr(v) for v in getattr(self, attr)]
                value = '[%s]' % ', '.join(sublist)
            elif attr_type == dict:
                sublist = ['%s=%r' % (k, v) for k, v in
                           sorted(getattr(self, attr).items(),
                                  key=lambda x: x[0])]
                value = '{%s}' % ', '.join(sublist)
            else:
                value = repr(getattr(self, attr))

            base.append('%s=%s' % (attr, value))

        return '<%s at 0x%x>' % (' '.join(base), id(self))

    @classmethod
    def hydrate(cls, db, limit):
        """
        Given a limit dict, as generated by dehydrate(), generate an
        appropriate instance of Limit (or a subclass).  If the
        required limit class cannot be found, returns None.
        """

        # Extract the limit name from the keyword arguments
        cls_name = limit.pop('limit_class')

        # Is it in the registry yet?
        if cls_name not in cls._registry:
            utils.find_entrypoint(None, cls_name)

        # Look it up in the registry
        cls = cls._registry.get(cls_name)

        # Instantiate the thing
        return cls(db, **limit) if cls else None

    def dehydrate(self):
        """Return a dict representing this limit."""

        # Only concerned about very specific attributes
        result = dict(limit_class=self._limit_full_name)
        for attr in self.attrs:
            # Using getattr allows the properties to come into play
            result[attr] = getattr(self, attr)

        return result

    def _route(self, mapper):
        """
        Set up the route(s) corresponding to the limit.  This controls
        which limits are checked against the request.

        :param mapper: The routes.Mapper object to add the route to.
        """

        # Build up the keyword arguments to feed to connect()
        kwargs = dict(conditions=dict(function=self._filter))

        # Restrict the verbs
        if self.verbs:
            kwargs['conditions']['method'] = self.verbs

        # Add requirements, if provided
        if self.requirements:
            kwargs['requirements'] = self.requirements

        # Hook to allow subclasses to override arguments to connect()
        uri = self.route(self.uri, kwargs)

        # Create the route
        mapper.connect(None, uri, **kwargs)

    def route(self, uri, route_args):
        """
        Provides a hook by which additional arguments may be added to
        the route.  For most limits, this should not be needed; use
        the filter() method instead.  Should return the URI to
        connect.

        :param uri: The configured URI.  May be returned unchanged.
        :param route_args: A dictionary of keyword arguments that will
                           be passed to routes.Mapper.connect().  This
                           dictionary should be modified in place.
        """

        return uri

    def load(self, key):
        """
        Given a bucket key, load the corresponding bucket.

        :param key: The bucket key.  This may be either a string or a
                    BucketKey object.

        :returns: A Bucket object.
        """

        # Turn the key into a BucketKey
        if isinstance(key, basestring):
            key = BucketKey.decode(key)

        # Make sure the uuids match
        if key.uuid != self.uuid:
            raise ValueError("%s is not a bucket corresponding to this limit" %
                             key)

        # If the key is a version 1 key, load it straight from the
        # database
        if key.version == 1:
            raw = self.db.get(str(key))
            if raw is None:
                return self.bucket_class(self.db, self, str(key))
            return self.bucket_class.hydrate(self.db, msgpack.loads(raw),
                                             self, str(key))

        # OK, use a BucketLoader
        records = self.db.lrange(str(key), 0, -1)
        loader = BucketLoader(self.bucket_class, self.db, self, str(key),
                              records)

        return loader.bucket

    def decode(self, key):
        """
        Given a bucket key, compute the parameters used to compute
        that key.

        Note: Deprecated.  Use BucketKey.decode() instead.

        :param key: The bucket key.  Note that the UUID must match the
                    UUID of this limit; a ValueError will be raised if
                    this is not the case.
        """

        # Parse the bucket key
        key = BucketKey.decode(key)

        # Make sure the uuids match
        if key.uuid != self.uuid:
            raise ValueError("%s is not a bucket corresponding to this limit" %
                             key)

        return key.params

    def key(self, params):
        """
        Given a set of parameters describing the request, compute a
        key for accessing the corresponding bucket.

        :param params: A dictionary of parameters describing the
                       request; this is likely based on the dictionary
                       from routes.
        """

        return str(BucketKey(self.uuid, params))

    def _filter(self, environ, params):
        """
        Performs final filtering of the request to determine if this
        limit applies.  Returns False if the limit does not apply or
        if the call should not be limited, or True to apply the limit.
        """

        # Search for required query arguments
        if self.queries:
            # No query string available
            if 'QUERY_STRING' not in environ:
                return False

            # Extract the list of provided query arguments from the
            # QUERY_STRING
            available = set(qstr.partition('=')[0] for qstr in
                            environ['QUERY_STRING'].split('&'))

            # Check if we have the required query arguments
            required = set(self.queries)
            if not required.issubset(available):
                return False

        # Use only the parameters listed in use; we'll add the others
        # back later
        unused = {}
        for key, value in params.items():
            if key not in self.use:
                unused[key] = value

        # Do this in a separate step so we avoid changing a
        # dictionary during traversal
        for key in unused:
            del params[key]

        # First, we need to set up any additional params required to
        # get the bucket.  If the DeferLimit exception is thrown, no
        # further processing is performed.
        try:
            additional = self.filter(environ, params, unused) or {}
        except DeferLimit:
            return False

        # Compute the bucket key
        key = self.key(params)

        # Update the parameters...
        params.update(unused)
        params.update(additional)

        # Get the current time
        now = time.time()

        # Allow up to a minute to mutate the bucket record.  If no
        # bucket exists currently, this is essentially a no-op, and
        # the bucket won't expire anyway, once the update record is
        # pushed.
        self.db.expire(key, 60)

        # Push an update record
        update_uuid = str(uuid.uuid4())
        update = {
            'uuid': update_uuid,
            'update': {
                'params': params,
                'time': now,
            },
        }
        self.db.rpush(key, msgpack.dumps(update))

        # Now suck in the bucket
        records = self.db.lrange(key, 0, -1)
        loader = BucketLoader(self.bucket_class, self.db, self, key, records)

        # Determine if we should initialize the compactor algorithm on
        # this bucket
        if 'turnstile.conf' in environ:
            config = environ['turnstile.conf']['compactor']
            try:
                max_updates = int(config['max_updates'])
            except (KeyError, ValueError):
                max_updates = None
            try:
                max_age = int(config['max_age'])
            except (KeyError, ValueError):
                max_age = 600
            if max_updates and loader.need_summary(now, max_updates, max_age):
                # Add a summary record; we want to do this before
                # instructing the compactor to compact.  If we did the
                # compactor instruction first, and a crash occurred
                # before adding the summarize record, the lack of
                # quiesence could cause two compactor threads to run
                # on the same bucket, leading to a race condition that
                # could corrupt the bucket.  With this ordering, if a
                # crash occurs before the compactor instruction, the
                # maximum aging applied to summarize records will
                # cause this logic to eventually be retriggered, which
                # should allow the compactor instruction to be issued.
                summarize = dict(summarize=now, uuid=str(uuid.uuid4()))
                self.db.rpush(key, msgpack.dumps(summarize))

                # Instruct the compactor to compact this record
                compactor_key = config.get('compactor_key', 'compactor')
                self.db.zadd(compactor_key, int(math.ceil(now)), key)

        # Set the expire on the bucket
        self.db.expireat(key, loader.bucket.expire)

        # If we found a delay, store the particulars in the
        # environment; this will later be sorted and an error message
        # corresponding to the longest delay returned.
        if loader.delay is not None:
            environ.setdefault('turnstile.delay', [])
            environ['turnstile.delay'].append((loader.delay, self,
                                               loader.bucket))

        # Finally, if desired, add the bucket key to a desired
        # database set
        set_name = environ.get('turnstile.bucket_set')
        if set_name:
            self.db.zadd(set_name, loader.bucket.expire, key)

        # Should we continue the route scan?
        return not self.continue_scan

    def filter(self, environ, params, unused):
        """
        Performs final route filtering.  Should add additional
        parameters to the `params` dict that should be used when
        looking up the bucket.  Parameters that should be added to
        params, but which should not be used to look up the bucket,
        may be returned as a dictionary.  If this limit should not be
        applied to this request, raise DeferLimit.  Note that
        parameters that have already been filtered out of `params`
        will be present in the `unused` dictionary, and will be
        automatically added back to `params` after generation of the
        key.

        Note that the Turnstile configuration is available in the
        environment under the "turnstile.conf" key.
        """

        pass  # Pragma: nocover

    def format(self, status, headers, environ, bucket, delay):
        """
        Formats a response entity.  Returns a tuple of the desired
        status code and the formatted entity.  The default status code
        is passed in, as is a dictionary of headers.

        :param status: The default status code.  Should be returned to
                       the caller, or an alternate selected.  The
                       status code should include both the number and
                       the message, separated by a single space.
        :param headers: A dictionary of headers for the response.
                        Should update the 'Content-Type' header at a
                        minimum.
        :param environ: The WSGI environment for the request.
        :param bucket: The bucket containing the data which caused the
                       delay decision to be made.  This can be used to
                       obtain such information as the next time the
                       request can be made.
        :param delay: The number of seconds by which the request
                      should be delayed.
        """

        # This is a default response entity, which can be overridden
        # by limit subclasses.
        entity = ("This request was rate-limited.  "
                  "Please retry your request after %s." %
                  time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(bucket.next)))
        headers['Content-Type'] = 'text/plain'

        return status, entity

    @property
    def value(self):
        """Retrieve the value for this limit."""

        return self._value

    @value.setter
    def value(self, value):
        """Change the value for this limit."""

        if value <= 0:
            raise ValueError("Limit value must be > 0")

        self._value = value

    @property
    def unit(self):
        """Retrieve the name of the unit used for this limit."""

        return str(self._unit)

    @unit.setter
    def unit(self, value):
        """
        Change the unit for this limit to the specified unit.  The new
        value may be specified as an integer, a string indicating the
        number of seconds, or one of the recognized unit names.
        """

        self._unit = TimeUnit(value)

    @property
    def unit_value(self):
        """
        Retrieve the unit used for this limit as an integer number of
        seconds.
        """

        return int(self._unit)

    @unit_value.setter
    def unit_value(self, value):
        """
        Change the unit for this limit to the specified unit.  The new
        value may be specified as an integer, a string indicating the
        number of seconds, or one of the recognized unit names.
        """

        self._unit = TimeUnit(value)

    @property
    def cost(self):
        """
        Retrieve the amount by which a request increases the water
        level in the bucket.
        """

        return float(self.unit_value) / float(self.value)
