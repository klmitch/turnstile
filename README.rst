==============================================
Turnstile Distributed Rate-Limiting Middleware
==============================================

Turnstile is a piece of WSGI middleware that performs true distributed
rate-limiting.  System administrators can run an API on multiple
nodes, then place this middleware in the pipeline prior to the
application.  Turnstile uses a Redis database to track the rate at
which users are hitting the API, and can then apply configured rate
limits, even if each request was made against a different API node.

Adding and Configuring Turnstile
================================

Turnstile is intended for use with PasteDeploy-style configuration
files.  It is a filter, and should be placed in an appropriate place
in the WSGI pipeline such that the limit classes used with Turnstile
can access the information necessary to make rate-limiting decisions.
(With the ``turnstile.limits:Limit`` class provided by Turnstile, no
additional information is required, as that class does not
differentiate between users of your application.)

The filter section of the PasteDeploy configuration file will also
need to contain enough information to allow Turnstile to access the
database.  Other options may be configured from here as well, such as
the ``preprocess`` configuration variable.  The simplest example of
Turnstile configuration would be::

    [filter:turnstile]
    paste.filter_factory = turnstile.middleware:turnstile_filter
    redis.host = <your Redis database host name or IP>

The following are the recognized configuration options:

control.channel
  Specifies the channel that the control daemon listens on.  (See
  below for more information about the purpose of the control daemon.)
  This option defaults to "control".

control.errors_channel
  Specifies the channel that the control daemon (see below) reports
  errors to.  This option defaults to "errors".

control.errors_key
  Specifies the key of a set in the Redis database to which errors
  will be stored.  This option defaults to "errors".

control.limits_key
  The key under which the limits are stored in the database.  See the
  section on tools for more information on how to load and dump the
  limits stored in the Redis database.  This option defaults to
  "limits".

control.node_name
  The name of the node.  If provided, this option allows the
  specification of a recognizable name for the node.  Currently, this
  node name is only reported when issuing a "ping" command to the
  control daemon (see below), and may be used to verify that all hosts
  responded to the ping.

control.reload_spread
  When limits are changed in the database, a command is sent to the
  control daemon (see below) to cause the limits to be reloaded.  As
  having all nodes hit the Redis database simultaneously may overload
  the database, this option, if set, allows the reload to be spread
  out randomly within a configured interval.  This option should be
  set to the size of the desired interval, in seconds.  If not set,
  limits will be reloaded immediately by all nodes.

control.shard_hint
  Can be used to set a sharding hint which will be provided to the
  listening thread of the control daemon (see below).  This hint is
  not used by the default Redis ``Connection`` class.

preprocess
  Contains a list of functions, specified as "module:function" pairs
  separated by spaces.  During each request, each preprocessor will be
  called in turn, with the middleware object (from which can be
  obtained the database handle, as well as the configuration as a
  two-layer dictionary) and the request environment as arguments.
  Note that any exceptions thrown by the preprocessors will not be
  caught, and request processing will be halted; this will likely
  result in a 500 error being returned to the user.

redis.connection_pool
  Identifies the connection pool class to use.  If not provided,
  defaults to ``redis.ConnectionPool``.  This may be used to allow
  client-side sharding of the Redis database.

redis.connection_pool.connection_class
  Identifies the connection class to use.  If not provided, the
  appropriate ``redis.Connection`` subclass for the configured
  connection is used (``redis.Connection`` if ``redis.host`` is
  specified, else ``redis.UnixDomainSocketConnection``).

redis.connection_pool.max_connections
  Allows specification of the maximum number of connections to the
  Redis database.  Optional.

redis.connection_pool.parser_class
  Identifies the parser class to use.  Optional.  This is an advanced
  feature of the ``redis`` package used by Turnstile.

redis.connection_pool.*
  Any other configuration value provided in the
  ``redis.connection_pool.`` hierarchy will be passed as keyword
  arguments to the configured connection pool class.  Note that the
  values passed will be strings.

redis.db
  Identifies the specific sub-database of the Redis database to be
  used by Turnstile.  If not provided, defaults to 0.

redis.host
  Identifies the host name or IP address of the Redis database to
  connect to.  Either ``redis.host`` or ``redis.unix_socket_path``
  must be provided.

redis.password
  If the Redis database has been configured to use a password, this
  option allows that password to be specified.

redis.port
  Identifies the port the Redis database is listening on.  If not
  provided, defaults to 6379.

redis.socket_timeout
  If provided, specifies an integer socket timeout for the Redis
  database connection.

redis.unix_socket_path
  Names the UNIX socket on the local host for the local Redis database
  to connect to.  Either ``redis.host`` or ``redis.unix_socket_path``
  must be provided.

status
  Contains the status code to return if rate limiting is tripped.
  This defaults to "413 Request Entity Too Large".  Note that this
  value must start with the 3-digit HTTP code, followed by a space and
  the text corresponding to that status code.  Also note that,
  regardless of the status code, Turnstile will include the
  ``Retry-After`` header in the response.  (The value of the
  ``Retry-After`` header will be the integer number of seconds until
  the request can be retried.)

Other configuration values are available to the preprocessors and the
``turnstile.limits:Limit`` subclasses, but extreme care should be
taken that such configurations remain in sync across the entire
cluster.

The Control Daemon
==================

Turnstile stores the limits configuration in the Redis database, in
addition to the ephemeral information used to check and enforce the
rate limits.  This makes it possible to change the limits dynamically
from a single, central location.  In order to facilitate such changes,
each Turnstile instance uses an eventlet thread to run a "control
daemon."  The control daemon uses the publish/subscribe support
provided by Redis to listen for commands, of which two are currently
recognized: ping and reload.

The Ping Command
----------------

The "ping" command is the simplest of the control daemon commands.  In
its simplest form, the message "ping:<channel>" is written to the control
channel, which will cause all running Turnstile instances to return
the message "pong" to the specified channel.  If the
``control.node_name`` configuration option has been set, this node
name will be included in the response, as "pong:<node name>".
Finally, additional data (such as a timestamp) can be included in the
"ping" command, as in the message "ping:<channel>:<timestamp>"; this
data will be appended to the response, i.e., "pong:<node
name>:<timestamp>".  This could be used to verify that all nodes are
responding and not too heavily loaded.

(Note that if ``control.node_name`` is not specified, the response to
a "ping" command containing additional data such as a timestamp will
be "pong::<timestamp>".)

The Reload Command
------------------

The "reload" command is the real reason for the existence of the
control daemon.  This command causes the current set of limits to be
loaded from the database and installed in the middleware.  (This
operation is done in a thread-safe manner.)

The simplest form of the reload command is simply, "reload".  If the
``control.reload_spread`` configuration option was set, the reload
will be scheduled for some time within the configured time interval;
otherwise, it will be performed immediately.

The next simplest form of the reload command is "reload:immediate".
This causes an immediate reload of the limits, overriding any
configured time spread.

The final form of the reload command is "reload:spread:<interval>",
where the "<interval>" specifies a time interval, in seconds, over
which to spread reloading of the limits.  This specified interval is
used in preference to that specified by ``control.reload_spread``, if
set.

Note that the ``setup_limits`` tool automatically initiates a reload
once the limits are updated in the database.  See the section on tools
for more information.

Turnstile Tools
===============

The limits are stored in the Redis database using a sorted set, and
they are encoded using Msgpack.  (Although the Msgpack format is not
human-readable, it is very space and time efficient, which is why it
was chosen for this application.)  This makes manual management of
them more difficult, and so Turnstile ships with two tools to make
management of the rate limiting configuration easier.

The ``dump_limits`` Tool
------------------------

The ``dump_limits`` tool may be used to dump the current limits in the
database into an XML representation.  This tool requires the name of
an INI-style configuration file; see the section on configuring the
tools below for more information.

A usage summary for ``dump_limits``::

  usage: dump_limits [-h] [--debug] config limits_file

  Dump the current limits from the Redis database.

  positional arguments:
    config       Name of the configuration file, for connecting to the Redis
                 database.
    limits_file  Name of the XML file that the limits will be dumped to.

  optional arguments:
    -h, --help   show this help message and exit
    --debug, -d  Run the tool in debug mode.

The ``setup_limits`` Tool
-------------------------

The ``setup_limits`` tool may be used to read an XML file (such as
that produced by ``dump_limits``) and load the rate limiting
configuration into the Redis database.  This tools also requires the
name of an INI-style configuration file; see the section on
configuring the tools below for more information.

A usage summary for ``setup_limits``::

  usage: setup_limits [-h] [--debug] [--dryrun] [--noreload]
                      [--reload-immediate] [--reload-spread SECS]
                      config limits_file

  Set up or update limits in the Redis database.

  positional arguments:
    config                Name of the configuration file, for connecting to the
                          Redis database.
    limits_file           Name of the XML file describing the limits to
                          configure.

  optional arguments:
    -h, --help            show this help message and exit
    --debug, -d           Run the tool in debug mode.
    --dryrun, --dry_run, --dry-run, -n
                          Perform a dry run; inhibits loading data into the
                          database.
    --noreload, -R        Inhibit issuing a reload command.
    --reload-immediate, -r
                          Cause all nodes to immediately reload the limits
                          configuration.
    --reload-spread SECS, -s SECS
                          Cause all nodes to reload the limits configuration
                          over the specified number of seconds.

Configuring the Tools
---------------------

Both ``dump_limits`` and ``setup_limits`` require an INI-style
configuration file, which specifies how to connect to the Redis
database.  This file should contain the section "[connection]" and
should be populated with the same "redis.*" options as the PasteDeploy
configuration file, minus the "redis." prefix.  For example::

    [connection]
    host = <your Redis database host name or IP>

Each "redis.*" option recognized by the Turnstile middleware is
understood by the tools.

Rate Limit XML
--------------

The XML file used for expressing rate limit configuration is
relatively straightforward, or at least as straightforward as XML can
be.  The top-level element is "<limits>"; this should contain a
sequence of "<limit>" elements, each containing a number of "<attr>"
elements.  The specific attributes available for any given limit class
depend on the exact class, but that information is documented in the
``attrs`` attribute of the limit class.  (This information is suitable
for introspection.)

The "<limit>" element has one XML attribute which must be specified:
the "class" attribute, which must be set to a "module:class" string
identifying the desired limit class.  The "<attr>" element also has a
single XML attribute which must be set: "name", which identifies the
name of the Limit attribute.  The contents of the "<attr>" element
identify the value for the named attribute.

Some limit attributes are lists; for these attributes, the "<attr>"
element must contain one or more "<value>" elements, whose contents
identify a single item in the attribute list.  Other limit attributes
are dictionaries; for these attributes, again the "<attr>" element
must contain one or more "<value>" elements, but now those "<value>"
elements must have the XML attribute "key" set to the dictionary key
corresponding to that value.

As an example, consider the following limits configuration::

    <?xml version='1.0' encoding='UTF-8'?>
    <limits>
      <limit class="turnstile.limits:Limit">
        <attr name="requirements">
	  <value key="pageid">[0-9]+</value>
	</attr>
        <attr name="unit">second</attr>
	<attr name="uri">/page/{pageid}</attr>
	<attr name="value">10</attr>
	<attr name="verbs">
	  <value>GET</value>
	</attr>
      </limit>
    </limits>

In this example, GET access to "/page/{pageid}" is rate-limited to 10
per second.  The ``requirements`` attribute may be used to specify
regular expressions to tune the matching of URI components; in this
case, the "{pageid}" value must be composed of 1 or more digits.  The
limit class used is the basic ``turnstile.limits:Limit`` limit class.

Custom Limit Classes
====================

All limit classes must descend from ``turnstile.limits:Limit``.  This
admittedly un-Pythonic requirement has a number of advantages,
including the specific machinery which allows limits to be stored into
the Redis database.  Most limit classes only need to worry about the
``attrs`` class attribute and the ``filter()`` method, although the
``route()`` and ``format()`` methods may also be hooked.  For more
information about these methods, see the documentation provided for
their default implementations in ``turnstile.limits:Limit``.

Accessing the Turnstile Configuration
=====================================

The Turnstile configuration is available to preprocessors and to the
Limit classes.  For preprocessors, it is available directly from the
middleware object (the first passed parameter) via the 'config'
attribute.  (The database handle is also available via the 'db'
attribute, should access to the database be required.)  For the
``filter()`` method of the Limit classes, the configuration is
available in the request environment under the "turnstile.config" key.

The Turnstile configuration is represented as a two-level dictionary.
Configuration keys that do not contain a '.' are available in the
dictionary accessible via the key 'None' in the base configuration.
For example, to obtain the configured status value, assuming the
Turnstile configuration is available in the "config" variable, the
correct code would be::

    status = config[None]['status']

For those configuration keys which do contain a '.', the part of the
name to the left of the first '.' becomes the first key, and the
remainder of the name is the second key.  For example, to access the
value of the "redis.connection_pool.connection_class" variable, the
correct code would be::

    connection_class = config['redis']['connection_pool.connection_class']

All values in the configuration are stored as strings.  Configuration
values do not need to be pre-declared in any way; Turnstile ignores
(but maintains) configuration values that it does not use, making
these values available for use by preprocessors and Limit classes.
