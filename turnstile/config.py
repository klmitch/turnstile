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

import ConfigParser

from turnstile import database


_str_true = set(['t', 'true', 'on', 'y', 'yes'])
_str_false = set(['f', 'false', 'off', 'n', 'no'])


class Config(object):
    """
    Stores configuration data.  Configuration can be loaded from the
    paste file (as "<sect>.<key> = <value>") or from a standard INI
    file.  For paste files, keys with no section prefix are made
    accessible as attributes; for standard INI files, the keys present
    in the "[turnstile]" section are made accessible as attributes.  A
    configuration file may be specified in the paste file with the
    special "config" key.  All keys associated with other sections are
    stored in dictionaries; these dictionaries are made accessible
    using subscripting.

    As an example, consider the following paste.ini file:

        [filter:turnstile]
        paste.filter_factory = turnstile.middleware:turnstile_filter
        preprocess = my_preproc:preproc
        redis.host = 10.0.0.1
        config = /etc/my_turnstile.conf

    Further assume that the /etc/my_turnstile.conf contains the
    following:

        [turnstile]
        status = 500 Internal Error

        [redis]
        password = s3cureM3!

        [control]
        node_name = mynode

    With this configuration, the config object acts like so:

        >>> config.preprocess
        'my_preproc:preproc'
        >>> config.status
        '500 Internal Error'
        >>> config.config
        '/etc/my_turnstile.conf'
        >>> config['redis']
        {'host': '10.0.0.1', 'password': 's3cureM3!'}
        >>> config['control']
        {'node_name': 'mynode'}
    """

    def __init__(self, conf_dict=None, conf_file=None):
        """
        Initializes a Config object.  A default is provided for the
        "status" configuration.

        :param conf_dict: Optional.  Should specify a dictionary
                          containing the configuration drawn from the
                          paste.ini file.  If a 'config' key is
                          present in the dict, configuration will
                          additionally be drawn from the specified INI
                          file; configuration from the INI file will
                          override configuration drawn from this dict.
        :param conf_file: Optional.  Should specify the name of a file
                          containing further configuration.  If a
                          conf_dict is also provided, values drawn
                          from this file will override values from the
                          conf_dict, as well as any additional file
                          specified by the 'config' key.

        For configuration files, values in the '[turnstile]' section
        correspond to prefix-less values in the dictionary, with the
        exception that the 'config' value is ignored.
        """

        self._config = {
            None: {
                'status': '413 Request Entity Too Large',
                },
            }

        # Handle passed-in dict (middleware)
        if conf_dict:
            for key, value in conf_dict.items():
                outer, _sep, inner = key.partition('.')

                # Deal with prefix-less keys
                if not inner:
                    outer, inner = None, outer

                # Make sure we have a place to put them
                self._config.setdefault(outer, {})
                self._config[outer][inner] = value

        conf_files = []

        # Were we to look aside to a configuration file?
        if 'config' in self._config[None]:
            conf_files.append(self._config[None]['config'])

        # Were we asked to load a specific file in addition?
        if conf_file:
            conf_files.append(conf_file)

        # Parse configuration files
        if conf_files:
            cp = ConfigParser.SafeConfigParser()
            cp.read(conf_files)

            # Each section corresponds to a top-level in the config
            for sect in cp.sections():
                # Handle the 'connection' section specially, for
                # backwards-compatibility
                if sect == 'connection':
                    self._config.setdefault('redis', {})
                    self._config.setdefault('control', {})

                    for key, value in cp.items(sect):
                        if key == 'limits_key':
                            self._config['control']['limits_key'] = value
                        elif key == 'control_channel':
                            self._config['control']['channel'] = value
                        else:
                            self._config['redis'][key] = value
                    continue

                # Transform [turnstile] section
                outer = None if sect == 'turnstile' else sect

                self._config.setdefault(outer, {})

                # Merge in the options from the section
                self._config[outer].update(dict(cp.items(sect)))

    def __str__(self):
        return str(self._config)

    def __getitem__(self, key):
        """
        Retrieve the configuration dictionary for the given section.
        If the section does not exist in the configuration, an empty
        dictionary is returned, for convenience.
        """

        return self._config.get(key, {})

    def __contains__(self, key):
        """
        Test if the given section exists in the configuration.
        Returns True if it does, False if it does not.  Note that
        __getitem__() returns an empty dictionary if __contains__()
        would return False.
        """

        return key in self._config

    def __getattr__(self, key):
        """
        Retrieve the given configuration option.  Configuration
        options that can be queried this way are those that are
        specified without prefix in the paste.ini file, or which are
        specified in the '[turnstile]' section of the configuration
        file.  Raises an AttributeError if the given option does not
        exist.
        """

        try:
            return self._config.get(None, {})[key]
        except KeyError:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, key))

    def get(self, key, default=None):
        """
        Retrieve the given configuration option.  Configuration
        options that can be queried this way are those that are
        specified without prefix in the paste.ini file, or which are
        specified in the '[turnstile]' section of the configuration
        file.  Returns the default value (None if not specified) if
        the given option does not exist.
        """

        return self._config.get(None, {}).get(key, default)

    def get_database(self, override=None):
        """
        Convenience function for obtaining a handle to the Redis
        database.  By default, uses the connection options from the
        '[redis]' section.  However, if the override parameter is
        given, it specifies a section containing overrides for the
        Redis connection info; the keys will all be prefixed with
        'redis.'.  For example, in the following configuration file:

            [redis]
            host = 10.0.0.1
            password = s3cureM3!

            [control]
            redis.host = 127.0.0.1

        A call to get_database() would return a handle for the redis
        database on 10.0.0.1, while a call to get_database('control')
        would return a handle for the redis database on 127.0.0.1; in
        both cases, the database password would be 's3cureM3!'.
        """

        # Grab the database connection arguments
        redis_args = self['redis']

        # If we have an override, read some overrides from that
        # section
        if override:
            redis_args = redis_args.copy()
            for key, value in self[override].items():
                if not key.startswith('redis.'):
                    continue
                redis_args[key[len('redis.'):]] = value

        # Return the redis database connection
        return database.initialize(redis_args)

    @staticmethod
    def to_bool(value, do_raise=True):
        """Convert a string to a boolean value.

        If the string consists of digits, the integer value of the string
        is coerced to a boolean value.  Otherwise, any of the strings "t",
        "true", "on", "y", and "yes" are considered True and any of the
        strings "f", "false", "off", "n", and "no" are considered False.
        A ValueError will be raised for any other value.
        """

        value = value.lower()

        # Try it as an integer
        if value.isdigit():
            return bool(int(value))

        # OK, check it against the true/false values...
        if value in _str_true:
            return True
        elif value in _str_false:
            return False

        # Not recognized
        if do_raise:
            raise ValueError("invalid literal for to_bool(): %r" % value)

        return False
