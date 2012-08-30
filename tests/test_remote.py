import errno
import json
import socket
import time
import warnings

import eventlet

from turnstile import control
from turnstile import remote

import tests
from tests import db_fixture


class FakeSocket(object):
    def __init__(self, *msgs, **kwargs):
        self._send_data = []
        self._recv_data = []
        self._actions = []
        self._fail = kwargs.get('fail')
        self._clients = kwargs.get('clients', [])
        self._closed = False

        if msgs:
            self._recv_data.extend(msgs)

    def _check_closed(self):
        if self._closed:
            raise socket.error(errno.EBADF, 'Bad file descriptor')

    def close(self):
        self._check_closed()
        self._closed = True

    def sendall(self, msg):
        self._check_closed()
        self._send_data.append(msg)

    def recv(self, size):
        self._check_closed()
        if not self._recv_data:
            self._closed = True
            return ''
        return self._recv_data.pop(0)

    def setsockopt(self, *args, **kwargs):
        self._actions.append(('setsockopt', args, kwargs))
        if self._fail == 'setsockopt':
            raise socket.error("setsockopt error occurred")

    def bind(self, *args, **kwargs):
        self._actions.append(('bind', args, kwargs))
        if self._fail == 'bind':
            raise socket.error("bind error occurred")

    def listen(self, *args, **kwargs):
        self._actions.append(('listen', args, kwargs))
        if self._fail == 'listen':
            raise socket.error("listen error occurred")

    def accept(self):
        if not self._clients:
            raise socket.error("out of clients")
        tmp = self._clients.pop(0)
        if isinstance(tmp, Exception):
            raise tmp
        return tmp, ('localhost', 1023)


class TestConnection(tests.TestCase):
    def test_send_open(self):
        sock = FakeSocket()
        conn = remote.Connection(sock)

        conn.send('FOO', 'Nobody', 'inspects', 'the', 'spammish', 'repetition')

        self.assertEqual(sock._send_data, [
                '{"cmd": "FOO", "payload": ["Nobody", "inspects", "the", '
                '"spammish", "repetition"]}\n'
                ])

    def test_send_closed(self):
        conn = remote.Connection(None)

        self.assertRaises(remote.ConnectionClosed, conn.send,
                          'FOO', 'Nobody', 'inspects', 'the', 'spammish',
                          'repetition')

    def test_send_error(self):
        sock = FakeSocket()
        sock.close()  # Make it raise an error
        conn = remote.Connection(sock)

        self.assertRaises(socket.error, conn.send, 'FOO', 'Nobody',
                          'inspects', 'the', 'spammish', 'repetition')
        self.assertEqual(conn._sock, None)

    def test_recvbuf_pop_msg(self):
        conn = remote.Connection(None)
        conn._recvbuf.append(dict(cmd='FOO', payload=['Nobody', 'inspects',
                                                      'the', 'spammish',
                                                      'repetition']))
        result = conn._recvbuf_pop()

        self.assertEqual(result, ('FOO', ['Nobody', 'inspects', 'the',
                                          'spammish', 'repetition']))

    def test_recvbuf_pop_exc(self):
        conn = remote.Connection(None)
        conn._recvbuf.append(Exception())
        self.assertRaises(Exception, conn._recvbuf_pop)

    def test_close(self):
        sock = FakeSocket()
        conn = remote.Connection(sock)
        conn._recvbuf.append('foo')
        conn._recvbuf_partial = 'bar'

        conn.close()

        self.assertTrue(sock._closed)
        self.assertEqual(conn._sock, None)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_close_double(self):
        conn = remote.Connection(None)
        conn._recvbuf.append('foo')
        conn._recvbuf_partial = 'bar'

        conn.close()

        self.assertEqual(conn._sock, None)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def stub_recvbuf_pop(self):
        def fake_recvbuf_pop(self):
            # I don't want to bother putting real messages on the
            # queue
            return self._recvbuf.pop(0)

        self.stubs.Set(remote.Connection, '_recvbuf_pop', fake_recvbuf_pop)
        self.stubs.Set(json, 'loads', lambda x: x)

    def test_recv_recvbuf_filled(self):
        self.stub_recvbuf_pop()

        conn = remote.Connection(None)
        conn._recvbuf = ['foo', 'bar']

        result = conn.recv()

        self.assertEqual(result, 'foo')
        self.assertEqual(conn._recvbuf, ['bar'])

    def test_recv_recvbuf_empty_closed(self):
        self.stub_recvbuf_pop()

        conn = remote.Connection(None)

        self.assertRaises(remote.ConnectionClosed, conn.recv)

    def test_recv_onemsg(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('foobar\n')
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, 'foobar')
        self.assertEqual(conn._sock, sock)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_recv_onemsg_existing_partial(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('bar\n')
        conn = remote.Connection(sock)
        conn._recvbuf_partial = 'foo'

        result = conn.recv()

        self.assertEqual(result, 'foobar')
        self.assertEqual(conn._sock, sock)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_recv_onemsg_onepartial(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('bar\nfoo')
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, 'bar')
        self.assertEqual(conn._sock, sock)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, 'foo')

    def test_recv_multimsg(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('foo\nbar\nbaz\n')
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, 'foo')
        self.assertEqual(conn._sock, sock)
        self.assertEqual(conn._recvbuf, ['bar', 'baz'])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_recv_longmsg(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('foo', 'bar', 'baz\n')
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, 'foobarbaz')
        self.assertEqual(conn._sock, sock)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_recv_closed(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket('foo')
        conn = remote.Connection(sock)

        self.assertRaises(remote.ConnectionClosed, conn.recv)
        self.assertEqual(conn._sock, None)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_recv_error(self):
        self.stub_recvbuf_pop()

        sock = FakeSocket()
        sock.close()  # Make it raise an error
        conn = remote.Connection(sock)

        self.assertRaises(socket.error, conn.recv)
        self.assertEqual(conn._sock, None)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')


class FakeConnection(object):
    def __init__(self, msgs=None):
        self._sendbuf = []
        self._recvbuf = msgs or []
        self._closed = False

    def _check_closed(self):
        if self._closed:
            raise remote.ConnectionClosed("Connection closed")

    def close(self):
        self._closed = True

    def send(self, cmd, *payload):
        self._check_closed()
        self._sendbuf.append(dict(cmd=cmd, payload=payload))

    def recv(self):
        msg = self._recvbuf.pop(0)
        if isinstance(msg, Exception):
            raise msg
        return msg['cmd'], msg['payload']


# Used for testing @remote
class FakeSimpleRPC(object):
    def __init__(self, mode, msgs=None):
        self.mode = mode
        self.conn = None
        self._msgs = msgs
        self._closed = False

    def close(self):
        self._closed = True

    def connect(self):
        self.conn = FakeConnection(self._msgs)

    @remote.remote
    def foobar(self, *args, **kwargs):
        assert self.mode == 'server'
        return args, kwargs


class TestException(Exception):
    pass


class TestRemote(tests.TestCase):
    imports = {
        'TestException': TestException,
        }

    def test_remote_attribute(self):
        rpc = FakeSimpleRPC(None)

        self.assertTrue(hasattr(rpc.foobar, '_remote'))
        self.assertTrue(rpc.foobar._remote)

    def test_remote_server(self):
        rpc = FakeSimpleRPC('server')

        result = rpc.foobar(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, ((1, 2, 3), dict(a=4, b=5, c=6)))
        self.assertEqual(rpc.conn, None)

    def test_remote_result(self):
        rpc = FakeSimpleRPC('client', [dict(cmd='RES', payload=['foobar'])])

        result = rpc.foobar(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, 'foobar')
        self.assertNotEqual(rpc.conn, None)
        self.assertEqual(rpc.conn._sendbuf, [
                dict(
                    cmd='CALL',
                    payload=('foobar', (1, 2, 3), dict(a=4, b=5, c=6)),
                    )])
        self.assertFalse(rpc._closed)

    def test_remote_complex_result(self):
        rpc = FakeSimpleRPC('client', [dict(cmd='RES', payload=[(3, 2, 1)])])

        result = rpc.foobar(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, (3, 2, 1))
        self.assertNotEqual(rpc.conn, None)
        self.assertEqual(rpc.conn._sendbuf, [
                dict(
                    cmd='CALL',
                    payload=('foobar', (1, 2, 3), dict(a=4, b=5, c=6)),
                    )])
        self.assertFalse(rpc._closed)

    def test_remote_exception(self):
        rpc = FakeSimpleRPC('client', [
                dict(
                    cmd='EXC',
                    payload=('TestException', 'this is a test'),
                    )])

        try:
            rpc.foobar()
            self.fail("Failed to raise TestException")
        except TestException as exc:
            self.assertEqual(str(exc), 'this is a test')
        self.assertFalse(rpc._closed)

    def test_remote_error(self):
        rpc = FakeSimpleRPC('client', [dict(cmd='ERR', payload=('wassup?',))])

        try:
            rpc.foobar()
            self.fail("Failed to raise Exception")
        except Exception as exc:
            self.assertEqual(str(exc),
                             'Catastrophic error from server: wassup?')
        self.assertTrue(rpc._closed)

    def test_remote_invalid(self):
        rpc = FakeSimpleRPC('client', [dict(cmd='FOO', payload=('wassup?',))])

        try:
            rpc.foobar()
            self.fail("Failed to raise Exception")
        except Exception as exc:
            self.assertEqual(str(exc),
                             "Invalid command response from server: FOO")
        self.assertTrue(rpc._closed)


class TestCreateServer(tests.TestCase):
    def setUp(self):
        super(TestCreateServer, self).setUp()

        self.socks = []

        def fake_getaddrinfo(host, port, family, socktype):
            # Use comma-separated hosts
            if host:
                for h in host.split(','):
                    yield (h, socktype, 'tcp', '', (host, int(port)))

        def fake_socket(family, type_, proto):
            self.assertEqual(type_, socket.SOCK_STREAM)
            self.assertEqual(proto, 'tcp')

            # Fail if family is 'fail'
            if family == 'fail':
                raise socket.error("failed to get socket")

            kwargs = {}
            if family in ('setsockopt', 'bind', 'listen'):
                kwargs['fail'] = family

            # Get a fake socket
            sock = FakeSocket(**kwargs)
            self.socks.append(sock)

            return sock

        self.stubs.Set(socket, 'getaddrinfo', fake_getaddrinfo)
        self.stubs.Set(socket, 'socket', fake_socket)

    def test_noaddrs(self):
        try:
            remote._create_server('', '1023')
            self.fail("_create_server() failed to raise socket.error")
        except socket.error as exc:
            self.assertEqual(str(exc), 'getaddrinfo returns an empty list')
        self.assertEqual(self.socks, [])

    def test_fails(self):
        try:
            remote._create_server('fail,setsockopt,bind,listen', '1023')
            self.fail("_create_server() failed to raise socket.error")
        except socket.error as exc:
            self.assertEqual(str(exc), 'listen error occurred')

        self.assertEqual(len(self.socks), 3)
        for idx, failtype in enumerate(('setsockopt', 'bind', 'listen')):
            self.assertEqual(self.socks[idx]._fail, failtype)
            self.assertEqual(self.socks[idx]._closed, True)

    def test_succeeds(self):
        sock = remote._create_server('localhost', '1023')

        self.assertEqual(self.socks, [sock])
        self.assertEqual(sock._actions, [
                ('setsockopt', (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
                 {}),
                ('bind', (('localhost', 1023),), {}),
                ('listen', (1024,), {}),
                ])
        self.assertEqual(sock._closed, False)


class RPCforTest(remote.SimpleRPC):
    connection_class = FakeConnection

    remote_attr = "remote_attr"

    def notremote_func(self, *args, **kwargs):
        return ('notremote_func', args, kwargs)

    @remote.remote
    def remote_func(self, *args, **kwargs):
        if 'do_raise' in kwargs:
            raise TestException(kwargs['do_raise'])
        return ('remote_func', args, kwargs)


class TestSimpleRPC(tests.TestCase):
    def setUp(self):
        super(TestSimpleRPC, self).setUp()

        self.msgs = []
        self.addr = None

        def fake_create_connection(addr):
            self.addr = addr
            return self.msgs

        self.stubs.Set(socket, 'create_connection', fake_create_connection)

    def test_init(self):
        rpc = RPCforTest('localhost', 'port', 'authkey')

        self.assertEqual(rpc.host, 'localhost')
        self.assertEqual(rpc.port, 'port')
        self.assertEqual(rpc.authkey, 'authkey')
        self.assertEqual(rpc.mode, None)
        self.assertEqual(rpc.conn, None)

    def test_close(self):
        conn = FakeConnection()
        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.conn = conn

        rpc.close()

        self.assertEqual(rpc.conn, None)
        self.assertEqual(conn._closed, True)

    def test_close_redundant(self):
        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.close()

        self.assertEqual(rpc.conn, None)

    def test_connect_server(self):
        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.mode = 'server'

        self.assertRaises(ValueError, rpc.connect)
        self.assertEqual(rpc.mode, 'server')

    def test_connect_redundant(self):
        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.mode = 'client'
        rpc.conn = 'connected'

        rpc.connect()

        self.assertEqual(rpc.mode, 'client')
        self.assertEqual(rpc.conn, 'connected')

    def test_connect_authed(self):
        self.msgs.append(dict(cmd='OK', payload=()))

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.connect()

        self.assertEqual(rpc.mode, 'client')
        self.assertNotEqual(rpc.conn, None)
        self.assertEqual(rpc.conn._sendbuf,
                         [dict(cmd='AUTH', payload=('authkey',))])

    def test_connect_err(self):
        self.msgs.append(dict(cmd='ERR', payload=('failed to auth',)))

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.connect()

        self.assertEqual(rpc.mode, 'client')
        self.assertEqual(rpc.conn, None)
        self.assertEqual(self.log_messages, [
                "Failed to authenticate to localhost port port: "
                "failed to auth",
                ])

    def test_connect_valueerror(self):
        self.msgs.append(ValueError("Bogus message"))

        rpc = RPCforTest('localhost', 'port', 'authkey')

        self.assertRaises(ValueError, rpc.connect)
        self.assertEqual(rpc.mode, 'client')
        self.assertEqual(rpc.conn, None)
        self.assertEqual(self.log_messages, [
                "Received bogus response from server: Bogus message",
                ])

    def test_connect_connectionclosed(self):
        self.msgs.append(remote.ConnectionClosed("Connection closed"))

        rpc = RPCforTest('localhost', 'port', 'authkey')

        self.assertRaises(remote.ConnectionClosed, rpc.connect)
        self.assertEqual(rpc.mode, 'client')
        self.assertEqual(rpc.conn, None)
        self.assertEqual(self.log_messages, [
                "Connection closed while authenticating to server",
                ])

    def test_connect_otherexception(self):
        self.msgs.append(TestException("foobar"))

        rpc = RPCforTest('localhost', 'port', 'authkey')

        self.assertRaises(TestException, rpc.connect)
        self.assertEqual(rpc.mode, 'client')
        self.assertEqual(rpc.conn, None)
        self.assertEqual(len(self.log_messages), 1)
        self.assertTrue(self.log_messages[0].startswith(
                "Failed to authenticate to server"))

    def stub_for_connect(self):
        conn = FakeConnection(self.msgs)

        def fake_connect(inst):
            inst.conn = conn

        self.stubs.Set(RPCforTest, 'connect', fake_connect)

        return conn

    def test_ping(self):
        cur_time = time.time()

        def fake_time():
            return cur_time

        self.stubs.Set(time, 'time', fake_time)

        self.msgs.append(dict(cmd='PONG', payload=(cur_time - 60,)))
        conn = self.stub_for_connect()

        rpc = RPCforTest('localhost', 'port', 'authkey')

        result = rpc.ping()

        self.assertEqual(result, 60.0)
        self.assertEqual(rpc.conn, conn)
        self.assertEqual(conn._sendbuf,
                         [dict(cmd='PING', payload=(cur_time,))])

    def test_ping_fail(self):
        self.msgs.append(dict(cmd='FOO', payload=('hi there',)))
        conn = self.stub_for_connect()

        rpc = RPCforTest('localhost', 'port', 'authkey')

        self.assertRaises(Exception, rpc.ping)

    def test_listen_client(self):
        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.mode = 'client'

        self.assertRaises(ValueError, rpc.listen)
        self.assertEqual(rpc.mode, 'client')

    def test_listen(self):
        listen_sock = FakeSocket(clients=[None, Exception('foo'), None])

        def fake_create_server(host, port):
            return listen_sock

        def fake_spawn_n(serve, conn, addr):
            self.assertEqual(serve.im_class, RPCforTest)
            self.assertIsInstance(conn, FakeConnection)
            self.assertEqual(addr, ('localhost', 1023))

        self.stubs.Set(remote, '_create_server', fake_create_server)
        self.stubs.Set(eventlet, 'spawn_n', fake_spawn_n)

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.listen()

        self.assertEqual(rpc.mode, 'server')
        self.assertEqual(rpc.conn, None)
        self.assertEqual(len(self.log_messages), 3)
        self.assertEqual(self.log_messages[0],
                         'Accepted connection from localhost port 1023')
        self.assertEqual(self.log_messages[1],
                         'Accepted connection from localhost port 1023')
        self.assertTrue(self.log_messages[2].startswith(
                'Too many errors accepting connections: out of clients'))
        self.assertEqual(listen_sock._closed, True)

    def test_serve_badparse_unauth(self):
        conn = FakeConnection([
                ValueError("Bad parse"),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='ERR',
                     payload=("Failed to parse command: Bad parse",)),
                ])
        self.assertEqual(self.log_messages, [
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_badparse_auth(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                ValueError("Bad parse"),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='ERR',
                     payload=("Failed to parse command: Bad parse",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_badauth(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('badauth',)),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='ERR', payload=("Invalid authentication key",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('badauth',)",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_doubleauth(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='AUTH', payload=('badauth',)),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='ERR', payload=("Already authenticated",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('badauth',)",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_unauth(self):
        conn = FakeConnection([
                dict(cmd='PING', payload=(11111,)),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='ERR', payload=("Not authenticated",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'PING' from 127.0.0.1 port 1023; payload: "
                "(11111,)",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_ping(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='PING', payload=(11111,)),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='PONG', payload=(11111,)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'PING' from 127.0.0.1 port 1023; payload: "
                "(11111,)",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_badpayload(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=()),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='ERR', payload=(
                        "Invalid payload for 'CALL' command: "
                        "need more than 0 values to unpack",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "()",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_nosuch(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=('nosuch_func', (1, 2), dict(a=4))),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='EXC', payload=('exceptions:AttributeError',
                                         "'RPCforTest' object has no "
                                         "attribute 'nosuch_func'")),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "('nosuch_func', (1, 2), {'a': 4})",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_attr(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=('remote_attr', (1, 2), dict(a=4))),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='EXC', payload=('exceptions:AttributeError',
                                         "'RPCforTest' object has no "
                                         "attribute 'remote_attr'")),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "('remote_attr', (1, 2), {'a': 4})",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_noremote(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=('noremote_func', (1, 2), dict(a=4))),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='EXC', payload=('exceptions:AttributeError',
                                         "'RPCforTest' object has no "
                                         "attribute 'noremote_func'")),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "('noremote_func', (1, 2), {'a': 4})",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_raise(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=('remote_func', (1, 2),
                                          dict(a=4, do_raise="testing"))),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.mode = 'server'

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='EXC', payload=('tests.test_remote:TestException',
                                         "testing")),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "('remote_func', (1, 2), {'a': 4, 'do_raise': 'testing'})",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_call_result(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='CALL', payload=('remote_func', (1, 2), dict(a=4))),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')
        rpc.mode = 'server'

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='RES', payload=(('remote_func', (1, 2), dict(a=4)),)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'CALL' from 127.0.0.1 port 1023; payload: "
                "('remote_func', (1, 2), {'a': 4})",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_unknown(self):
        conn = FakeConnection([
                dict(cmd='AUTH', payload=('authkey',)),
                dict(cmd='XXXX', payload=()),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [
                dict(cmd='OK', payload=()),
                dict(cmd='ERR', payload=("Unrecognized command 'XXXX'",)),
                ])
        self.assertEqual(self.log_messages, [
                "Received command 'AUTH' from 127.0.0.1 port 1023; payload: "
                "('authkey',)",
                "Received command 'XXXX' from 127.0.0.1 port 1023; payload: "
                "()",
                "Received command 'QUIT' from 127.0.0.1 port 1023; payload: "
                "()",
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_closed(self):
        conn = FakeConnection([
                remote.ConnectionClosed("Connection closed"),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [])
        self.assertEqual(self.log_messages, [
                "Closing connection from 127.0.0.1 port 1023",
                ])

    def test_serve_exception(self):
        conn = FakeConnection([
                TestException("test exception"),
                dict(cmd='QUIT', payload=()),
                ])

        rpc = RPCforTest('localhost', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1023))

        self.assertEqual(conn._closed, True)
        self.assertEqual(conn._sendbuf, [])
        self.assertEqual(len(self.log_messages), 2)
        self.assertTrue(self.log_messages[0].startswith(
                'Error serving client at 127.0.0.1 port 1023: test exception'))
        self.assertEqual(self.log_messages[1],
                         "Closing connection from 127.0.0.1 port 1023")


class FakeControlDaemon(object):
    def __init__(self, limits=[]):
        self.limits = db_fixture.FakeLimitData(limits)


class TestControlDaemonRPC(tests.TestCase):
    def test_init(self):
        rpc = remote.ControlDaemonRPC('localhost', 'port', 'authkey',
                                      'daemon')

        self.assertEqual(rpc.host, 'localhost')
        self.assertEqual(rpc.port, 'port')
        self.assertEqual(rpc.authkey, 'authkey')
        self.assertEqual(rpc.daemon, 'daemon')
        self.assertEqual(rpc.mode, None)
        self.assertEqual(rpc.conn, None)

    def test_get_limits(self):
        limits = ["Nobody", "inspects", "the", "spammish", "repetition"]
        daemon = FakeControlDaemon(limits)
        rpc = remote.ControlDaemonRPC('localhost', 'port', 'authkey',
                                      daemon)
        rpc.mode = 'server'

        result = rpc.get_limits(0)

        self.assertEqual(result, (len(limits), limits))

    def test_get_limits_unchanged(self):
        limits = ["Nobody", "inspects", "the", "spammish", "repetition"]
        daemon = FakeControlDaemon(limits)
        rpc = remote.ControlDaemonRPC('localhost', 'port', 'authkey',
                                      daemon)
        rpc.mode = 'server'

        self.assertRaises(control.NoChangeException, rpc.get_limits, 5)


class TestRemoteLimitData(tests.TestCase):
    def test_set_limits(self):
        rpc = db_fixture.FakeLimitData()  # Not RPC, but works...
        ld = remote.RemoteLimitData(rpc)

        self.assertRaises(ValueError, ld.set_limits, ['limit', 'data'])

    def test_get_limits(self):
        rpc = db_fixture.FakeLimitData(["Nobody", "inspects", "the",
                                        "spammish", "repetition"])
        ld = remote.RemoteLimitData(rpc)

        result = ld.get_limits()

        self.assertEqual(result, (5, ["Nobody", "inspects", "the",
                                      "spammish", "repetition"]))

    def test_get_limits_nochange(self):
        rpc = db_fixture.FakeLimitData(["Nobody", "inspects", "the",
                                        "spammish", "repetition"])
        ld = remote.RemoteLimitData(rpc)

        self.assertRaises(control.NoChangeException, ld.get_limits, 5)

    def test_get_limits_error(self):
        rpc = db_fixture.FakeLimitData([TestException("haha")])
        ld = remote.RemoteLimitData(rpc)

        self.assertRaises(control.NoChangeException, ld.get_limits)


class FakeControlDaemonRPC(tests.GenericFakeClass):
    def __init__(self, *args, **kwargs):
        super(FakeControlDaemonRPC, self).__init__(*args, **kwargs)
        self._listening = False

    def listen(self):
        self._listening = True


class TestRemoteControlDaemon(tests.TestCase):
    def setUp(self):
        super(TestRemoteControlDaemon, self).setUp()

        self.stubs.Set(remote, 'ControlDaemonRPC', FakeControlDaemonRPC)

    def test_init(self):
        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                'remote.authkey': 'fakeauth',
                })
        daemon = remote.RemoteControlDaemon(tests.FakeMiddleware(conf), conf)

        self.assertIsInstance(daemon.remote, FakeControlDaemonRPC)
        self.assertEqual(daemon.remote.args, ())
        self.assertEqual(daemon.remote.kwargs, dict(
                daemon=daemon,
                host='127.0.0.1',
                port=5779,
                authkey='fakeauth',
                ))
        self.assertEqual(daemon.remote_limits, None)

    def test_init_nohost(self):
        conf = tests.FakeConfig({
                'remote.port': '5779',
                'remote.authkey': 'fakeauth',
                })
        with warnings.catch_warnings(record=True) as w:
            self.assertRaises(ValueError,
                              remote.RemoteControlDaemon,
                              tests.FakeMiddleware(conf), conf)
            self.assertIn("Missing value for configuration key "
                          "'control.remote.host'", w[-1].message)

    def test_init_noport(self):
        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.authkey': 'fakeauth',
                })
        with warnings.catch_warnings(record=True) as w:
            self.assertRaises(ValueError,
                              remote.RemoteControlDaemon,
                              tests.FakeMiddleware(conf), conf)
            self.assertIn("Missing value for configuration key "
                          "'control.remote.port'", w[-1].message)

    def test_init_badport(self):
        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': 'badport',
                'remote.authkey': 'fakeauth',
                })
        with warnings.catch_warnings(record=True) as w:
            self.assertRaises(ValueError,
                              remote.RemoteControlDaemon,
                              tests.FakeMiddleware(conf), conf)
            self.assertIn("Invalid value for configuration key "
                          "'control.remote.port'", w[-1].message)

    def test_init_noauth(self):
        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                })
        with warnings.catch_warnings(record=True) as w:
            self.assertRaises(ValueError,
                              remote.RemoteControlDaemon,
                              tests.FakeMiddleware(conf), conf)
            self.assertIn("Missing value for configuration key "
                          "'control.remote.authkey'", w[-1].message)

    def test_get_limits_existing(self):
        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                'remote.authkey': 'authkey',
                })
        daemon = remote.RemoteControlDaemon(tests.FakeMiddleware(conf), conf)
        daemon.remote_limits = 'remote'

        result = daemon.get_limits()

        self.assertEqual(result, 'remote')
        self.assertEqual(daemon.remote_limits, 'remote')

    def test_get_limits_new(self):
        self.stubs.Set(remote, 'RemoteLimitData', tests.GenericFakeClass)

        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                'remote.authkey': 'authkey',
                })
        daemon = remote.RemoteControlDaemon(tests.FakeMiddleware(conf), conf)

        result = daemon.get_limits()

        self.assertEqual(result, daemon.remote_limits)
        self.assertIsInstance(result, tests.GenericFakeClass)
        self.assertEqual(result.args, (daemon.remote,))

    def test_start(self):
        self.spawned = False
        self.reloaded = False

        def fake_spawn_n(method, *args, **kwargs):
            self.spawned = (method, args, kwargs)
            return self.spawned

        def fake_reload(inst):
            self.reloaded = True

        self.stubs.Set(eventlet, 'spawn_n', fake_spawn_n)
        self.stubs.Set(remote.RemoteControlDaemon, 'reload', fake_reload)

        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                'remote.authkey': 'fakeauth',
                })
        daemon = remote.RemoteControlDaemon(tests.FakeMiddleware(conf), conf)

        daemon.start()

        # The start method must do nothing
        self.assertEqual(self.spawned, False)
        self.assertEqual(self.reloaded, False)

    def test_serve(self):
        self.started = False

        def fake_start(inst):
            self.started = True

        self.stubs.Set(control.ControlDaemon, 'start', fake_start)

        conf = tests.FakeConfig({
                'remote.host': '127.0.0.1',
                'remote.port': '5779',
                'remote.authkey': 'fakeauth',
                })
        daemon = remote.RemoteControlDaemon(tests.FakeMiddleware(conf), conf)

        daemon.serve()

        self.assertEqual(self.started, True)
        self.assertEqual(daemon.remote._listening, True)
