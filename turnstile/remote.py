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

import functools
import json
import logging
import socket
import sys
import time
import warnings

import eventlet

from turnstile import control
from turnstile import utils


LOG = logging.getLogger('turnstile')


class ConnectionClosed(Exception):
    """Raised to indicate a connection has been closed."""

    pass


class Connection(object):
    """Buffered network connection."""

    def __init__(self, sock):
        """Initialize a Connection object."""

        self._sock = sock
        self._recvbuf = []
        self._recvbuf_partial = ''

    def close(self):
        """
        Close the connection.

        :param purge: If True (the default), the receive buffer will
                      be purged.
        """

        # Close the underlying socket
        if self._sock:
            with utils.ignore_except():
                self._sock.close()
            self._sock = None

        # Purge the message buffers
        self._recvbuf = []
        self._recvbuf_partial = ''

    def send(self, cmd, *payload):
        """
        Send a command message to the other end.

        :param cmd: The command to send to the other end.
        :param payload: The command payload.  Note that all elements
                        of the payload must be serializable to JSON.
        """

        # If it's closed, raise an error up front
        if not self._sock:
            raise ConnectionClosed("Connection closed")

        # Construct the outgoing message
        msg = json.dumps(dict(cmd=cmd, payload=payload)) + '\n'

        # Send it
        try:
            self._sock.sendall(msg)
        except socket.error:
            # We'll need to re-raise
            e_type, e_value, e_tb = sys.exc_info()

            # Make sure the socket is closed
            self.close()

            # Re-raise
            raise e_type, e_value, e_tb

    def _recvbuf_pop(self):
        """
        Internal helper to pop a message off the receive buffer.  If
        the message is an Exception, that exception will be raised;
        otherwise, a tuple of command and payload will be returned.
        """

        # Pop a message off the recv buffer and return (or raise) it
        msg = self._recvbuf.pop(0)
        if isinstance(msg, Exception):
            raise msg
        return msg['cmd'], msg['payload']

    def recv(self):
        """
        Receive a message from the other end.  Returns a tuple of the
        command (a string) and payload (a list).
        """

        # See if we have a message to process...
        if self._recvbuf:
            return self._recvbuf_pop()

        # If it's closed, don't try to read more data
        if not self._sock:
            raise ConnectionClosed("Connection closed")

        # OK, get some data from the socket
        while True:
            try:
                data = self._sock.recv(4096)
            except socket.error:
                # We'll need to re-raise
                e_type, e_value, e_tb = sys.exc_info()

                # Make sure the socket is closed
                self.close()

                # Re-raise
                raise e_type, e_value, e_tb

            # Did the connection get closed?
            if not data:
                # There can never be anything in the buffer here
                self.close()
                raise ConnectionClosed("Connection closed")

            # Begin parsing the read-in data
            partial = self._recvbuf_partial + data
            self._recvbuf_partial = ''
            while partial:
                msg, sep, partial = partial.partition('\n')

                # If we have no sep, then it's not a complete message,
                # and the remainder is in msg
                if not sep:
                    self._recvbuf_partial = msg
                    break

                # Parse the message
                try:
                    self._recvbuf.append(json.loads(msg))
                except ValueError as exc:
                    # Error parsing the message; save the exception,
                    # which we will re-raise
                    self._recvbuf.append(exc)

            # Make sure we have a message to return
            if self._recvbuf:
                return self._recvbuf_pop()

            # We have no complete messages; loop around and try to
            # read more data
            continue


def remote(func):
    """
    Decorator to mark a function as invoking a remote procedure call.
    When invoked in server mode, the function will be called; when
    invoked in client mode, an RPC will be initiated.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.mode == 'server':
            # In server mode, call the function
            return func(self, *args, **kwargs)

        # Make sure we're connected
        if not self.conn:
            self.connect()

        # Call the remote function
        self.conn.send('CALL', func.__name__, args, kwargs)

        # Receive the response
        cmd, payload = self.conn.recv()
        if cmd == 'ERR':
            self.close()
            raise Exception("Catastrophic error from server: %s" %
                            payload[0])
        elif cmd == 'EXC':
            exc_type = utils.import_class(payload[0])
            raise exc_type(payload[1])
        elif cmd != 'RES':
            self.close()
            raise Exception("Invalid command response from server: %s" % cmd)

        return payload[0]

    # Mark it a callable
    wrapper._remote = True

    # Return the wrapped function
    return wrapper


def _create_server(host, port):
    """
    Helper function.  Creates a listening socket on the designated
    host and port.  Modeled on the socket.create_connection()
    function.
    """

    exc = socket.error("getaddrinfo returns an empty list")
    for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        sock = None
        try:
            # Create the listening socket
            sock = socket.socket(af, socktype, proto)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(sa)
            sock.listen(1024)
            return sock

        except socket.error as exc:
            # Clean up after ourselves
            if sock is not None:
                sock.close()

    # Couldn't create a listening socket
    raise exc


class SimpleRPC(object):
    """
    Implements simple remote procedure call.  When run in client mode
    (by calling the connect() method), designated remote calls will be
    submitted to the server (specified by the arguments to the
    constructor).  When run in server mode (by calling the listen()
    method), client connections are accepted, and client requests
    handled by calling the requested functions.

    Note: The connection is not secured through cryptographic means.
    Clients authenticate by sending an authkey to the server, which
    must match the authkey used by the server.  It is strongly
    recommended that this class only be used on the local host.
    """

    connection_class = Connection

    def __init__(self, host, port, authkey):
        """
        Initialize a SimpleRPC object.

        :param host: The host the server will listen on.  It is
                     strongly recommended that this always be
                     "127.0.0.1", since no cryptography is used.
        :param port: The TCP port the server will listen on.
        :param authkey: An authentication key.  The server and all
                        clients must use the same authentication key.
        """

        self.host = host
        self.port = port
        self.authkey = authkey

        self.mode = None
        self.conn = None

    def close(self):
        """
        Close the connection to the server.
        """

        # Close the connection
        if self.conn:
            self.conn.close()
        self.conn = None

    def ping(self):
        """
        Ping the server.  Returns the time interval, in seconds,
        required for the server to respond to the PING message.
        """

        # Make sure we're connected
        if not self.conn:
            self.connect()

        # Send the ping and wait for the response
        self.conn.send('PING', time.time())
        cmd, payload = self.conn.recv()
        recv_ts = time.time()

        # Make sure the response was a PONG
        if cmd != 'PONG':
            raise Exception("Invalid response from server")

        # Return the RTT
        return recv_ts - payload[0]

    def connect(self):
        """
        Connect to the server.  This method causes the SimpleRPC
        object to switch to client mode.  Note some methods, such as
        the ping() method, implicitly call this method.
        """

        # Make sure we're in client mode
        if self.mode and self.mode != 'client':
            raise ValueError("%s is not in client mode" %
                             self.__class__.__name__)
        self.mode = 'client'

        # If we're connected, nothing to do
        if self.conn:
            return

        # OK, attempt the connection
        fd = socket.create_connection((self.host, self.port))

        # Initialize the connection object
        self.conn = self.connection_class(fd)

        # Authenticate
        try:
            self.conn.send('AUTH', self.authkey)
            cmd, payload = self.conn.recv()
            if cmd != 'OK':
                LOG.error("Failed to authenticate to %s port %s: %s" %
                          (self.host, self.port, payload[0]))
                self.close()
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()

            # Log the error
            if exc_type == ValueError:
                LOG.error("Received bogus response from server: %s" %
                          str(exc_value))
            elif exc_type == ConnectionClosed:
                LOG.error("%s while authenticating to server" %
                          str(exc_value))
            else:
                LOG.exception("Failed to authenticate to server")

            # Close the connection
            self.close()

            # Re-raise the exception
            raise exc_type, exc_value, exc_tb

    def listen(self):
        """
        Listen for clients.  This method causes the SimpleRPC object
        to switch to server mode.  One thread will be created for each
        client.
        """

        # Make sure we're in server mode
        if self.mode and self.mode != 'server':
            raise ValueError("%s is not in server mode" %
                             self.__class__.__name__)
        self.mode = 'server'

        # Obtain a listening socket
        serv = _create_server(self.host, self.port)

        # If we have too many errors, we want to bail out
        err_thresh = 0
        while True:
            # Accept a connection
            try:
                sock, addr = serv.accept()
            except Exception as exc:
                err_thresh += 1
                if err_thresh > 10:
                    LOG.exception("Too many errors accepting "
                                  "connections: %s" % str(exc))
                    break
                continue

            # Decrement error count on successful connections
            err_thresh = max(err_thresh - 1, 0)

            # Log the connection attempt
            LOG.info("Accepted connection from %s port %s" %
                     (addr[0], addr[1]))

            # And handle the connection
            eventlet.spawn_n(self.serve, self.connection_class(sock), addr)

        # Close the listening socket
        with utils.ignore_except():
            serv.close()

    def serve(self, conn, addr):
        """
        Handle a single client.

        :param conn: The Connection instance.
        :param addr: The address of the client, for logging purposes.
        """

        try:
            # Handle data from the client
            auth = False
            while True:
                # Get the command
                try:
                    cmd, payload = conn.recv()
                except ValueError as exc:
                    # Tell the client about the error
                    conn.send('ERR', "Failed to parse command: %s" % str(exc))

                    # If they haven't successfully authenticated yet,
                    # disconnect them
                    if not auth:
                        return
                    continue

                # Log the command and payload, for debugging purposes
                LOG.debug("Received command %r from %s port %s; payload: %r" %
                          (cmd, addr[0], addr[1], payload))

                # Handle authentication
                if cmd == 'AUTH':
                    if auth:
                        conn.send('ERR', "Already authenticated")
                    elif payload[0] != self.authkey:
                        # Don't give them a second chance
                        conn.send('ERR', "Invalid authentication key")
                        return
                    else:
                        # Authentication successful
                        conn.send('OK')
                        auth = True

                # Special QUIT command for testing purposes
                elif cmd == 'QUIT':
                    return

                # Handle unauthenticated connections
                elif not auth:
                    # No second chances
                    conn.send('ERR', "Not authenticated")
                    return

                # Handle aliveness test
                elif cmd == 'PING':
                    conn.send('PONG', *payload)

                # Handle a function call command
                elif cmd == 'CALL':
                    try:
                        # Get the call parameters
                        try:
                            funcname, args, kwargs = payload
                        except ValueError as exc:
                            conn.send('ERR', "Invalid payload for 'CALL' "
                                      "command: %s" % str(exc))
                            continue

                        # Look up the function
                        func = getattr(self, funcname, None)
                        if (not func or
                            not callable(func) or
                            not getattr(func, '_remote', False)):
                            raise AttributeError(
                                "%r object has no attribute %r" %
                                (self.__class__.__name__, funcname))

                        # Call the function
                        result = func(*args, **kwargs)
                    except Exception as exc:
                        exc_name = '%s:%s' % (exc.__class__.__module__,
                                              exc.__class__.__name__)
                        conn.send('EXC', exc_name, str(exc))
                    else:
                        # Return the result
                        conn.send('RES', result)

                # Handle all other commands by returning an ERR
                else:
                    conn.send('ERR', "Unrecognized command %r" % cmd)

        except ConnectionClosed:
            # Ignore the connection closed error
            pass
        except Exception as exc:
            # Log other exceptions
            LOG.exception("Error serving client at %s port %s: %s" %
                          (addr[0], addr[1], str(exc)))
            pass

        finally:
            LOG.info("Closing connection from %s port %s" %
                     (addr[0], addr[1]))

            # Make sure the socket gets closed
            conn.close()


class ControlDaemonRPC(SimpleRPC):
    """
    A SimpleRPC subclass for use by the Turnstile control daemon.
    """

    def __init__(self, host, port, authkey, daemon):
        """
        Initialize a ControlDaemonRPC object.

        :param host: The host the server will listen on.  It is
                     strongly recommended that this always be
                     "127.0.0.1", since no cryptography is used.
        :param port: The TCP port the server will listen on.
        :param authkey: An authentication key.  The server and all
                        clients must use the same authentication key.
        :param daemon: The control daemon instance.
        """

        super(ControlDaemonRPC, self).__init__(host, port, authkey)
        self.daemon = daemon

    @remote
    def get_limits(self, limit_sum):
        """
        Retrieve a list of msgpack'd limit strings if the checksum is
        not the one given.  Raises turnstile.control.NoChangeException
        if the checksums match.
        """

        return self.daemon.limits.get_limits(limit_sum)


class RemoteLimitData(object):
    """
    Provides remote access to limit data stored in another process.
    This uses an RPC to obtain limit data maintained by the
    RemoteControlDaemon process.
    """

    def __init__(self, rpc):
        """
        Initialize RemoteLimitData.  Stores a reference to the RPC
        client object.
        """

        self.limit_rpc = rpc
        self.limit_lock = eventlet.semaphore.Semaphore()

    def set_limits(self, limits):
        """
        Remote limit data is treated as read-only (with external
        update).
        """

        raise ValueError("Cannot set remote limit data")

    def get_limits(self, limit_sum=None):
        """
        Gets the current limit data if it is different from the data
        indicated by limit_sum.  The db argument is used for hydrating
        the limit objects.  Raises a NoChangeException if the
        limit_sum represents no change, otherwise returns a tuple
        consisting of the current limit_sum and a list of Limit
        objects.
        """

        with self.limit_lock:
            # Grab the checksum and limit list
            try:
                return self.limit_rpc.get_limits(limit_sum)
            except control.NoChangeException:
                # Expected possibility
                raise
            except Exception:
                # Something happened; maybe the server isn't running.
                # Pretend that there's no change...
                raise control.NoChangeException()


class RemoteControlDaemon(control.ControlDaemon):
    """
    A daemon process which listens for control messages and can reload
    the limit configuration from the database.  Based on the
    ControlDaemon, but starts an RPC server to enable access to the
    limit data from multiple processes.
    """

    def __init__(self, middleware, conf):
        """
        Initialize the RemoteControlDaemon.
        """

        # Grab required configuration values
        required = {
            'remote.host': lambda x: x,
            'remote.port': int,
            'remote.authkey': lambda x: x,
            }
        values = {}
        for conf_key, xform in required.items():
            try:
                values[conf_key[len('remote.'):]] = \
                    xform(conf['control'][conf_key])
            except KeyError:
                warnings.warn("Missing value for configuration key "
                              "'control.%s'" % conf_key)
            except ValueError:
                warnings.warn("Invalid value for configuration key "
                              "'control.%s'" % conf_key)
            else:
                del required[conf_key]

        # Error out if we're missing something critical
        if required:
            raise ValueError("Missing required configuration for "
                             "RemoteControlDaemon.  Missing or invalid "
                             "configuration keys: %s" %
                             ', '.join(['control.%s' % k
                                        for k in sorted(required.keys())]))

        super(RemoteControlDaemon, self).__init__(middleware, conf)

        # Set up the RPC object
        self.remote = ControlDaemonRPC(daemon=self, **values)
        self.remote_limits = None

    def get_limits(self):
        """
        Retrieve the LimitData object the middleware will use for
        getting the limits.  This implementation returns a
        RemoteLimitData instance that can access the LimitData stored
        in the RemoteControlDaemon process.
        """

        # Set one up if we don't already have it
        if not self.remote_limits:
            self.remote_limits = RemoteLimitData(self.remote)
        return self.remote_limits

    def start(self):
        """
        Starts the RemoteControlDaemon.
        """

        # Don't connect the client yet, to avoid problems if we fork
        pass

    def serve(self):
        """
        Starts the RemoteControlDaemon process.  Forks a thread for
        listening to the Redis database, then initializes and starts
        the RPC server.
        """

        # Start the listening thread and load the limits
        super(RemoteControlDaemon, self).start()

        # Start the RPC server in this thread
        self.remote.listen()

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
