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

import json
import socket

import eventlet.semaphore
import mock
import unittest2

from turnstile import config
from turnstile import control
from turnstile import remote
from turnstile import utils

from tests.unit import utils as test_utils


class TestConnection(unittest2.TestCase):
    def test_init(self):
        conn = remote.Connection('socket')

        self.assertEqual(conn._sock, 'socket')
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

    def test_close(self):
        sock = mock.Mock()
        conn = remote.Connection(sock)
        conn._recvbuf = ['1', '2', '3']
        conn._recvbuf_partial = 'testing'

        conn.close()

        sock.close.assert_called_once_with()
        self.assertEqual(conn._sock, None)
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')

        # Make sure this doesn't raise an error
        conn.close()

    @mock.patch.object(remote.Connection, 'close')
    def test_send_closed(self, mock_close):
        conn = remote.Connection(None)

        self.assertRaises(remote.ConnectionClosed, conn.send, 'cmd', 'arg1',
                          'arg2')
        self.assertFalse(mock_close.called)

    @mock.patch('json.dumps')
    @mock.patch.object(remote.Connection, 'close')
    def test_send_success(self, mock_close, mock_dumps):
        def fake_dumps(obj):
            results = []
            for key, value in sorted(obj.items(), key=lambda x: x[0]):
                if isinstance(value, tuple):
                    value = list(value)
                if isinstance(value, dict):
                    results.append('%r: %s' % (key, fake_dumps(value)))
                else:
                    results.append('%r: %r' % (key, value))
            return '{%s}' % ', '.join(results)
        mock_dumps.side_effect = fake_dumps

        sock = mock.Mock()
        conn = remote.Connection(sock)

        conn.send('cmd', 'arg1', 'arg2')

        sock.sendall.assert_called_once_with(
            "{'cmd': 'cmd', 'payload': ['arg1', 'arg2']}\n")
        self.assertFalse(mock_close.called)

    @mock.patch('json.dumps')
    @mock.patch.object(remote.Connection, 'close')
    def test_send_failure(self, mock_close, mock_dumps):
        def fake_dumps(obj):
            results = []
            for key, value in sorted(obj.items(), key=lambda x: x[0]):
                if isinstance(value, tuple):
                    value = list(value)
                if isinstance(value, dict):
                    results.append('%r: %s' % (key, fake_dumps(value)))
                else:
                    results.append('%r: %r' % (key, value))
            return '{%s}' % ', '.join(results)
        mock_dumps.side_effect = fake_dumps

        sock = mock.Mock(**{'sendall.side_effect': socket.error()})
        conn = remote.Connection(sock)

        self.assertRaises(socket.error, conn.send, 'cmd', 'arg1', 'arg2')

        sock.sendall.assert_called_once_with(
            "{'cmd': 'cmd', 'payload': ['arg1', 'arg2']}\n")
        mock_close.assert_called_once_with()

    def test_recvbuf_pop(self):
        conn = remote.Connection(None)
        conn._recvbuf = [
            dict(cmd='cmd1', payload='payload1'),
            test_utils.TestException('testing exception'),
            dict(cmd='cmd2', payload='payload2'),
        ]

        self.assertEqual(conn._recvbuf_pop(), ('cmd1', 'payload1'))
        self.assertRaises(test_utils.TestException, conn._recvbuf_pop)
        self.assertEqual(conn._recvbuf_pop(), ('cmd2', 'payload2'))

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_buffered(self, mock_close):
        conn = remote.Connection(None)
        conn._recvbuf = [
            dict(cmd='cmd', payload='payload'),
        ]

        result = conn.recv()

        self.assertEqual(result, ('cmd', 'payload'))
        self.assertFalse(mock_close.called)

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_closed(self, mock_close):
        conn = remote.Connection(None)

        self.assertRaises(remote.ConnectionClosed, conn.recv)
        self.assertFalse(mock_close.called)

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_recverror(self, mock_close):
        sock = mock.Mock(**{'recv.side_effect': socket.error})
        conn = remote.Connection(sock)

        self.assertRaises(socket.error, conn.recv)
        sock.recv.assert_called_once_with(4096)
        mock_close.assert_called_once_with()

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_recvclosed(self, mock_close):
        sock = mock.Mock(**{'recv.return_value': ''})
        conn = remote.Connection(sock)

        self.assertRaises(remote.ConnectionClosed, conn.recv)
        sock.recv.assert_called_once_with(4096)
        mock_close.assert_called_once_with()

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_loop(self, mock_close):
        msgs = [
            json.dumps(dict(cmd='cmd1', payload='payload1')),
            json.dumps(dict(cmd='cmd2', payload='payload2')),
            json.dumps(dict(cmd='cmd3', payload='payload3')),
            json.dumps(dict(cmd='cmd4', payload='payload4')),
        ]
        msg_first_divide = len(msgs[0]) / 2
        msg_first_partial = msgs[0][:msg_first_divide]
        msgs[0] = msgs[0][msg_first_divide:]
        msg_last_divide = len(msgs[-1]) / 2
        msg_last_partial = msgs[-1][:msg_last_divide]
        msgs[-1] = msgs[-1][msg_last_divide:]
        sock = mock.Mock(**{'recv.return_value': '\n'.join(msgs)})
        conn = remote.Connection(sock)
        conn._recvbuf_partial = msg_first_partial

        result = conn.recv()

        self.assertEqual(result, ('cmd1', 'payload1'))
        self.assertEqual(conn._recvbuf, [
            dict(cmd='cmd2', payload='payload2'),
            dict(cmd='cmd3', payload='payload3'),
        ])
        self.assertEqual(conn._recvbuf_partial, msgs[-1])
        self.assertFalse(mock_close.called)

    @mock.patch.object(remote.Connection, 'close')
    @mock.patch('json.loads')
    def test_recv_exception(self, mock_loads, mock_close):
        expected = ValueError()
        mock_loads.side_effect = [
            dict(cmd='cmd', payload='payload'),
            expected,
        ]

        sock = mock.Mock(**{'recv.return_value': 'foo\nbar\n'})
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, ('cmd', 'payload'))
        self.assertEqual(conn._recvbuf, [expected])
        self.assertEqual(conn._recvbuf_partial, '')
        self.assertFalse(mock_close.called)

    @mock.patch.object(remote.Connection, 'close')
    def test_recv_partial(self, mock_close):
        msg = json.dumps(dict(cmd='cmd', payload='payload')) + '\n'
        msg_divide = len(msg) / 2
        sock = mock.Mock(**{'recv.side_effect': [
            msg[:msg_divide],
            msg[msg_divide:],
        ]})
        conn = remote.Connection(sock)

        result = conn.recv()

        self.assertEqual(result, ('cmd', 'payload'))
        self.assertEqual(conn._recvbuf, [])
        self.assertEqual(conn._recvbuf_partial, '')
        self.assertFalse(mock_close.called)


class RemoteTester(object):
    def __init__(self, mode=None, conn=None, post_conn=None):
        self.mode = mode
        self.conn = conn
        self.post_conn = post_conn

    def connect(self):
        self.conn = self.post_conn
        self.conn.connect()

    def close(self):
        self.conn.close()
        self.conn = None

    @remote.remote
    def func(self, *args, **kwargs):
        return 'func', args, kwargs

    def other(self):
        pass


class TestRemote(unittest2.TestCase):
    def test_marked(self):
        self.assertEqual(RemoteTester.func._remote, True)
        self.assertEqual(hasattr(RemoteTester.other, '_remote'), False)

    def test_server(self):
        rt = RemoteTester(mode='server')

        result = rt.func(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, ('func', (1, 2, 3), dict(a=4, b=5, c=6)))

    def test_connect(self):
        conn = mock.Mock(**{
            'recv.return_value': ('RES', ['result']),
        })
        rt = RemoteTester(post_conn=conn)

        result = rt.func(1, 2, 3, a=4, b=5, c=6)

        self.assertEqual(result, 'result')
        self.assertNotEqual(rt.conn, None)
        conn.assert_has_calls([
            mock.call.connect(),
            mock.call.send('CALL', 'func', (1, 2, 3), dict(a=4, b=5, c=6)),
            mock.call.recv(),
        ])
        self.assertEqual(len(conn.method_calls), 3)

    def test_error(self):
        conn = mock.Mock(**{
            'recv.return_value': ('ERR', ['test error']),
        })
        rt = RemoteTester(conn=conn)

        self.assertRaises(Exception, rt.func, 1, 2, 3, a=4, b=5, c=6)
        self.assertEqual(rt.conn, None)
        conn.assert_has_calls([
            mock.call.send('CALL', 'func', (1, 2, 3), dict(a=4, b=5, c=6)),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 3)

    def test_invalid(self):
        conn = mock.Mock(**{
            'recv.return_value': ('INVALID', ['test error']),
        })
        rt = RemoteTester(conn=conn)

        self.assertRaises(Exception, rt.func, 1, 2, 3, a=4, b=5, c=6)
        self.assertEqual(rt.conn, None)
        conn.assert_has_calls([
            mock.call.send('CALL', 'func', (1, 2, 3), dict(a=4, b=5, c=6)),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 3)

    @mock.patch.object(utils, 'find_entrypoint',
                       return_value=test_utils.TestException)
    def test_exception(self, mock_find_entrypoint):
        conn = mock.Mock(**{
            'recv.return_value': ('EXC', ['Test:Exception', 'message']),
        })
        rt = RemoteTester(conn=conn)

        self.assertRaises(test_utils.TestException, rt.func,
                          1, 2, 3, a=4, b=5, c=6)
        self.assertNotEqual(rt.conn, None)
        conn.assert_has_calls([
            mock.call.send('CALL', 'func', (1, 2, 3), dict(a=4, b=5, c=6)),
            mock.call.recv(),
        ])
        self.assertEqual(len(conn.method_calls), 2)
        mock_find_entrypoint.assert_called_once_with(None, 'Test:Exception')


class TestCreateServer(unittest2.TestCase):
    @mock.patch.object(socket, 'getaddrinfo', return_value=[])
    @mock.patch.object(socket, 'socket')
    def test_empty_list(self, mock_socket, mock_getaddrinfo):
        self.assertRaises(socket.error, remote._create_server, 'host', 'port')
        mock_getaddrinfo.assert_called_once_with('host', 'port', 0,
                                                 socket.SOCK_STREAM)
        self.assertFalse(mock_socket.called)

    @mock.patch.object(socket, 'getaddrinfo', return_value=[
        (socket.AF_INET6, socket.SOCK_STREAM, 0, '', ('::1', 1234)),
        (socket.AF_INET6, socket.SOCK_STREAM, 1, '', ('::2', 1234)),
        (socket.AF_INET, socket.SOCK_STREAM, 2, '', ('127.0.0.1', 1234)),
    ])
    @mock.patch.object(socket, 'socket', side_effect=socket.error)
    def test_all_fail_socket(self, mock_socket, mock_getaddrinfo):
        self.assertRaises(socket.error, remote._create_server, 'host', 'port')
        mock_getaddrinfo.assert_called_once_with('host', 'port', 0,
                                                 socket.SOCK_STREAM)
        mock_socket.assert_has_calls([
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 0),
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 1),
            mock.call(socket.AF_INET, socket.SOCK_STREAM, 2),
        ])

    @mock.patch.object(socket, 'getaddrinfo', return_value=[
        (socket.AF_INET6, socket.SOCK_STREAM, 0, '', ('::1', 1234)),
        (socket.AF_INET6, socket.SOCK_STREAM, 1, '', ('::2', 1234)),
        (socket.AF_INET, socket.SOCK_STREAM, 2, '', ('127.0.0.1', 1234)),
    ])
    @mock.patch.object(socket, 'socket')
    def test_all_fail_listen(self, mock_socket, mock_getaddrinfo):
        sockets = [
            mock.Mock(**{'listen.side_effect': socket.error}),
            mock.Mock(**{'listen.side_effect': socket.error}),
            mock.Mock(**{'listen.side_effect': socket.error}),
        ]
        mock_socket.side_effect = sockets
        self.assertRaises(socket.error, remote._create_server, 'host', 'port')
        mock_getaddrinfo.assert_called_once_with('host', 'port', 0,
                                                 socket.SOCK_STREAM)
        mock_socket.assert_has_calls([
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 0),
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 1),
            mock.call(socket.AF_INET, socket.SOCK_STREAM, 2),
        ])
        for idx, sock in enumerate(sockets):
            sock.assert_has_calls([
                mock.call.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                     1),
                mock.call.bind(mock_getaddrinfo.return_value[idx][4]),
                mock.call.listen(1024),
                mock.call.close(),
            ])

    @mock.patch.object(socket, 'getaddrinfo', return_value=[
        (socket.AF_INET6, socket.SOCK_STREAM, 0, '', ('::1', 1234)),
        (socket.AF_INET6, socket.SOCK_STREAM, 1, '', ('::2', 1234)),
        (socket.AF_INET, socket.SOCK_STREAM, 2, '', ('127.0.0.1', 1234)),
    ])
    @mock.patch.object(socket, 'socket')
    def test_succeed(self, mock_socket, mock_getaddrinfo):
        sockets = [
            mock.Mock(**{'listen.side_effect': socket.error}),
            mock.Mock(),
            mock.Mock(**{'listen.side_effect': socket.error}),
        ]
        mock_socket.side_effect = sockets
        result = remote._create_server('host', 'port')
        mock_getaddrinfo.assert_called_once_with('host', 'port', 0,
                                                 socket.SOCK_STREAM)
        mock_socket.assert_has_calls([
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 0),
            mock.call(socket.AF_INET6, socket.SOCK_STREAM, 1),
        ])
        sockets[0].assert_has_calls([
            mock.call.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                 1),
            mock.call.bind(mock_getaddrinfo.return_value[0][4]),
            mock.call.listen(1024),
            mock.call.close(),
        ])
        sockets[1].assert_has_calls([
            mock.call.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                 1),
            mock.call.bind(mock_getaddrinfo.return_value[1][4]),
            mock.call.listen(1024),
        ])
        self.assertFalse(sockets[1].close.called)
        self.assertEqual(result, sockets[1])
        self.assertEqual(len(sockets[2].method_calls), 0)


class TestSimpleRPC(unittest2.TestCase):
    def test_init(self):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertEqual(rpc.host, 'host')
        self.assertEqual(rpc.port, 'port')
        self.assertEqual(rpc.authkey, 'authkey')
        self.assertEqual(rpc.mode, None)
        self.assertEqual(rpc.conn, None)
        self.assertEqual(rpc.connection_class, remote.Connection)

    def test_close(self):
        conn = mock.Mock()
        rpc = remote.SimpleRPC('host', 'port', 'authkey')
        rpc.conn = conn

        rpc.close()

        self.assertEqual(rpc.conn, None)
        conn.close.assert_called_once_with()

        # Just check to see if it raises an error
        rpc.close()

    @mock.patch.object(remote.SimpleRPC, 'connect')
    @mock.patch('time.time', side_effect=[1000000.0, 1000010.0])
    def test_ping(self, mock_time, mock_connect):
        conn = mock.Mock(**{
            'recv.return_value': ('PONG', [1000005.0]),
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        def fake_connect():
            rpc.conn = conn
        mock_connect.side_effect = fake_connect

        result = rpc.ping()

        self.assertEqual(result, 5.0)
        mock_connect.assert_called_once_with()
        conn.assert_has_calls([
            mock.call.send('PING', 1000000.0),
            mock.call.recv(),
        ])

    @mock.patch.object(remote.SimpleRPC, 'connect')
    @mock.patch('time.time', side_effect=[1000000.0, 1000010.0])
    def test_ping_badresponse(self, mock_time, mock_connect):
        conn = mock.Mock(**{
            'recv.return_value': ('BAD', [1000005.0]),
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')
        rpc.conn = conn

        self.assertRaises(Exception, rpc.ping)

        conn.assert_has_calls([
            mock.call.send('PING', 1000000.0),
            mock.call.recv(),
        ])

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_badmode(self, mock_exception, mock_error, mock_close,
                             mock_connection_class, mock_create_connection):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')
        rpc.mode = 'server'

        self.assertRaises(ValueError, rpc.connect)
        self.assertFalse(mock_create_connection.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_connected(self, mock_exception, mock_error, mock_close,
                               mock_connection_class, mock_create_connection):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')
        rpc.mode = 'client'
        rpc.conn = 'connection'

        rpc.connect()

        self.assertFalse(mock_create_connection.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_authed(self, mock_exception, mock_error, mock_close,
                            mock_connection_class, mock_create_connection):
        conn = mock_connection_class.return_value
        conn.recv.return_value = ('OK', [])
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.connect()

        self.assertEqual(rpc.mode, 'client')
        mock_create_connection.assert_called_once_with(('host', 'port'))
        mock_connection_class.assert_called_once_with('connection')
        self.assertEqual(rpc.conn, conn)
        self.assertFalse(mock_close.called)
        self.assertFalse(mock_error.called)
        self.assertFalse(mock_exception.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_unauthed(self, mock_exception, mock_error, mock_close,
                              mock_connection_class, mock_create_connection):
        conn = mock_connection_class.return_value
        conn.recv.return_value = ('ERR', ['Bad authkey'])
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.connect()

        self.assertEqual(rpc.mode, 'client')
        mock_create_connection.assert_called_once_with(('host', 'port'))
        mock_connection_class.assert_called_once_with('connection')
        self.assertEqual(rpc.conn, conn)
        mock_error.assert_called_once_with(
            "Failed to authenticate to host port port: Bad authkey")
        mock_close.assert_called_once_with()
        self.assertFalse(mock_exception.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_badresponse(self, mock_exception, mock_error, mock_close,
                                 mock_connection_class,
                                 mock_create_connection):
        conn = mock_connection_class.return_value
        conn.recv.side_effect = ValueError('bogus response')
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(ValueError, rpc.connect)

        self.assertEqual(rpc.mode, 'client')
        mock_create_connection.assert_called_once_with(('host', 'port'))
        mock_connection_class.assert_called_once_with('connection')
        self.assertEqual(rpc.conn, conn)
        mock_error.assert_called_once_with(
            "Received bogus response from server: bogus response")
        mock_close.assert_called_once_with()
        self.assertFalse(mock_exception.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_closed(self, mock_exception, mock_error, mock_close,
                            mock_connection_class, mock_create_connection):
        conn = mock_connection_class.return_value
        conn.recv.side_effect = remote.ConnectionClosed("Connection closed")
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(remote.ConnectionClosed, rpc.connect)

        self.assertEqual(rpc.mode, 'client')
        mock_create_connection.assert_called_once_with(('host', 'port'))
        mock_connection_class.assert_called_once_with('connection')
        self.assertEqual(rpc.conn, conn)
        mock_error.assert_called_once_with(
            "Connection closed while authenticating to server")
        mock_close.assert_called_once_with()
        self.assertFalse(mock_exception.called)

    @mock.patch.object(socket, 'create_connection', return_value='connection')
    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       return_value=mock.Mock())
    @mock.patch.object(remote.SimpleRPC, 'close')
    @mock.patch.object(remote.LOG, 'error')
    @mock.patch.object(remote.LOG, 'exception')
    def test_connect_exception(self, mock_exception, mock_error, mock_close,
                               mock_connection_class, mock_create_connection):
        conn = mock_connection_class.return_value
        conn.recv.side_effect = test_utils.TestException
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(test_utils.TestException, rpc.connect)

        self.assertEqual(rpc.mode, 'client')
        mock_create_connection.assert_called_once_with(('host', 'port'))
        mock_connection_class.assert_called_once_with('connection')
        self.assertEqual(rpc.conn, conn)
        mock_exception.assert_called_once_with(
            "Failed to authenticate to server")
        mock_close.assert_called_once_with()
        self.assertFalse(mock_error.called)

    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       side_effect=lambda x: 'con:%s' % x)
    @mock.patch.object(remote, '_create_server')
    @mock.patch.object(remote.LOG, 'info')
    @mock.patch.object(remote.LOG, 'exception')
    @mock.patch('eventlet.spawn_n')
    def test_listen_badmode(self, mock_spawn_n, mock_exception, mock_info,
                            mock_create_server, mock_connection_class):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')
        rpc.mode = 'client'

        self.assertRaises(ValueError, rpc.listen)
        self.assertFalse(mock_create_server.called)

    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       side_effect=lambda x: 'con:%s' % x)
    @mock.patch.object(remote, '_create_server', return_value=mock.Mock())
    @mock.patch.object(remote.LOG, 'info')
    @mock.patch.object(remote.LOG, 'exception')
    @mock.patch('eventlet.spawn_n')
    def test_listen_noclients(self, mock_spawn_n, mock_exception, mock_info,
                              mock_create_server, mock_connection_class):
        serv = mock_create_server.return_value
        serv.accept.side_effect = test_utils.Halt
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        with utils.ignore_except():
            rpc.listen()

        self.assertEqual(rpc.mode, 'server')
        mock_create_server.assert_called_once_with('host', 'port')
        serv.assert_has_calls([
            mock.call.accept(),
        ])
        self.assertEqual(len(serv.method_calls), 1)
        self.assertFalse(mock_exception.called)
        self.assertFalse(mock_info.called)
        self.assertFalse(mock_connection_class.called)
        self.assertFalse(mock_spawn_n.called)

    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       side_effect=lambda x: 'con:%s' % x)
    @mock.patch.object(remote, '_create_server', return_value=mock.Mock())
    @mock.patch.object(remote.LOG, 'info')
    @mock.patch.object(remote.LOG, 'exception')
    @mock.patch('eventlet.spawn_n')
    def test_listen_clients(self, mock_spawn_n, mock_exception, mock_info,
                            mock_create_server, mock_connection_class):
        serv = mock_create_server.return_value
        serv.accept.side_effect = [
            ('sock0', ('127.0.0.1', 1234)),
            ('sock1', ('127.0.0.2', 4321)),
            test_utils.Halt,
        ]
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        with utils.ignore_except():
            rpc.listen()

        self.assertEqual(rpc.mode, 'server')
        mock_create_server.assert_called_once_with('host', 'port')
        serv.assert_has_calls([
            mock.call.accept(),
            mock.call.accept(),
            mock.call.accept(),
        ])
        self.assertEqual(len(serv.method_calls), 3)
        self.assertFalse(mock_exception.called)
        mock_info.assert_has_calls([
            mock.call('Accepted connection from 127.0.0.1 port 1234'),
            mock.call('Accepted connection from 127.0.0.2 port 4321'),
        ])
        mock_connection_class.assert_has_calls([
            mock.call('sock0'),
            mock.call('sock1'),
        ])
        mock_spawn_n.assert_has_calls([
            mock.call(rpc.serve, 'con:sock0', ('127.0.0.1', 1234)),
            mock.call(rpc.serve, 'con:sock1', ('127.0.0.2', 4321)),
        ])

    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       side_effect=lambda x: 'con:%s' % x)
    @mock.patch.object(remote.SimpleRPC, 'max_err_thresh', 3)
    @mock.patch.object(remote, '_create_server', return_value=mock.Mock())
    @mock.patch.object(remote.LOG, 'info')
    @mock.patch.object(remote.LOG, 'exception')
    @mock.patch('eventlet.spawn_n')
    def test_listen_errors(self, mock_spawn_n, mock_exception, mock_info,
                           mock_create_server, mock_connection_class):
        serv = mock_create_server.return_value
        serv.accept.side_effect = [
            test_utils.TestException("exc1"),
            test_utils.TestException("exc2"),
            test_utils.TestException("exc3"),
            test_utils.Halt,
        ]
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        try:
            rpc.listen()
        except test_utils.Halt:
            self.fail("listen() failed to break out of loop due to errors")

        self.assertEqual(rpc.mode, 'server')
        mock_create_server.assert_called_once_with('host', 'port')
        serv.assert_has_calls([
            mock.call.accept(),
            mock.call.accept(),
            mock.call.accept(),
            mock.call.close(),
        ])
        self.assertEqual(len(serv.method_calls), 4)
        mock_exception.assert_called_once_with(
            "Too many errors accepting connections: exc3")
        self.assertFalse(mock_info.called)
        self.assertFalse(mock_connection_class.called)
        self.assertFalse(mock_spawn_n.called)

    @mock.patch.object(remote.SimpleRPC, 'connection_class',
                       side_effect=lambda x: 'con:%s' % x)
    @mock.patch.object(remote.SimpleRPC, 'max_err_thresh', 3)
    @mock.patch.object(remote, '_create_server', return_value=mock.Mock())
    @mock.patch.object(remote.LOG, 'info')
    @mock.patch.object(remote.LOG, 'exception')
    @mock.patch('eventlet.spawn_n')
    def test_listen_errors_decrement(self, mock_spawn_n, mock_exception,
                                     mock_info, mock_create_server,
                                     mock_connection_class):
        serv = mock_create_server.return_value
        serv.accept.side_effect = [
            test_utils.TestException("exc1"),
            test_utils.TestException("exc2"),
            ('sock0', ('127.0.0.1', 1234)),
            test_utils.TestException("exc3"),
            test_utils.TestException("exc4"),
            test_utils.Halt,
        ]
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        try:
            rpc.listen()
        except test_utils.Halt:
            self.fail("listen() failed to break out of loop due to errors")

        self.assertEqual(rpc.mode, 'server')
        mock_create_server.assert_called_once_with('host', 'port')
        serv.assert_has_calls([
            mock.call.accept(),
            mock.call.accept(),
            mock.call.accept(),
            mock.call.accept(),
            mock.call.accept(),
            mock.call.close(),
        ])
        self.assertEqual(len(serv.method_calls), 6)
        mock_exception.assert_called_once_with(
            "Too many errors accepting connections: exc4")
        mock_info.assert_called_once_with(
            "Accepted connection from 127.0.0.1 port 1234")
        mock_connection_class.assert_called_once_with('sock0')
        mock_spawn_n.assert_called_once_with(rpc.serve, 'con:sock0',
                                             ('127.0.0.1', 1234))

    def test_get_remote_method_noattr(self):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(AttributeError, rpc._get_remote_method, 'nosuch')

    def test_get_remote_method_notcallable(self):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(AttributeError, rpc._get_remote_method,
                          'max_err_thresh')

    def test_get_remote_method_notremote(self):
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        self.assertRaises(AttributeError, rpc._get_remote_method, 'ping')

    def test_get_remote_method_remote(self):
        class TestRPC(remote.SimpleRPC):
            def func(self):
                pass
            func._remote = True

        rpc = TestRPC('host', 'port', 'authkey')

        self.assertEqual(rpc.func, rpc._get_remote_method('func'))

    @mock.patch.object(remote, 'LOG')
    def test_serve_closed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': remote.ConnectionClosed,
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 2)
        mock_LOG.assert_has_calls([
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 1)

    @mock.patch.object(remote, 'LOG')
    def test_serve_exception(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': test_utils.TestException("exception"),
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 2)
        mock_LOG.assert_has_calls([
            mock.call.exception(
                "Error serving client at 127.0.0.1 port 1234: exception"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_parsefailed_unauthed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': ValueError("bad command"),
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', 'Failed to parse command: bad command'),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 3)
        mock_LOG.assert_has_calls([
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 1)

    @mock.patch.object(remote, 'LOG')
    def test_serve_parsefailed_authed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ValueError("bad command"),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', 'Failed to parse command: bad command'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 1)

    @mock.patch.object(remote, 'LOG')
    def test_serve_auth_authed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('AUTH', ['authkey']),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', 'Already authenticated'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'AUTH' from 127.0.0.1 "
                            "port 1234; payload: ['authkey']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_auth_badkey(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('AUTH', ['foobar']),
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', 'Invalid authentication key'),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 3)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'AUTH' from 127.0.0.1 "
                            "port 1234; payload: ['foobar']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_auth_accepted(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('AUTH', ['authkey']),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('OK'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'AUTH' from 127.0.0.1 "
                            "port 1234; payload: ['authkey']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_ping_unauthed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('PING', ['some time']),
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234))

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', 'Not authenticated'),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 3)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'PING' from 127.0.0.1 "
                            "port 1234; payload: ['some time']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_ping_authed(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('PING', ['some time']),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('PONG', 'some time'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'PING' from 127.0.0.1 "
                            "port 1234; payload: ['some time']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    def test_serve_unrecognized(self, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('OTHER', ['some', 'arguments']),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', "Unrecognized command 'OTHER'"),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'OTHER' from 127.0.0.1 "
                            "port 1234; payload: ['some', 'arguments']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    @mock.patch.object(remote.SimpleRPC, '_get_remote_method')
    def test_serve_call_badpayload(self, mock_get_remote_method, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('CALL', ['funcname']),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('ERR', "Invalid payload for 'CALL' command: "
                           "need more than 1 value to unpack"),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'CALL' from 127.0.0.1 "
                            "port 1234; payload: ['funcname']"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    @mock.patch.object(remote.SimpleRPC, '_get_remote_method',
                       side_effect=AttributeError("no such attribute"))
    def test_serve_call_nomethod(self, mock_get_remote_method, mock_LOG):
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('CALL', ['funcname', (), {}]),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('EXC', 'exceptions:AttributeError',
                           'no such attribute'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'CALL' from 127.0.0.1 "
                            "port 1234; payload: ['funcname', (), {}]"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)

    @mock.patch.object(remote, 'LOG')
    @mock.patch.object(remote.SimpleRPC, '_get_remote_method',
                       return_value=mock.Mock())
    def test_serve_call_exception(self, mock_get_remote_method, mock_LOG):
        method = mock_get_remote_method.return_value
        method.side_effect = test_utils.TestException("some exception")
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('CALL', ['funcname', (), {}]),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('EXC', 'tests.unit.utils:TestException',
                           'some exception'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'CALL' from 127.0.0.1 "
                            "port 1234; payload: ['funcname', (), {}]"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)
        method.assert_called_once_with()

    @mock.patch.object(remote, 'LOG')
    @mock.patch.object(remote.SimpleRPC, '_get_remote_method',
                       return_value=mock.Mock())
    def test_serve_call_exception(self, mock_get_remote_method, mock_LOG):
        method = mock_get_remote_method.return_value
        method.return_value = "result"
        conn = mock.Mock(**{
            'recv.side_effect': [
                ('CALL', ['funcname', (1, 2, 3), dict(a=4)]),
                remote.ConnectionClosed,
            ],
        })
        rpc = remote.SimpleRPC('host', 'port', 'authkey')

        rpc.serve(conn, ('127.0.0.1', 1234), True)

        conn.assert_has_calls([
            mock.call.recv(),
            mock.call.send('RES', 'result'),
            mock.call.recv(),
            mock.call.close(),
        ])
        self.assertEqual(len(conn.method_calls), 4)
        mock_LOG.assert_has_calls([
            mock.call.debug("Received command 'CALL' from 127.0.0.1 "
                            "port 1234; payload: ['funcname', (1, 2, 3), "
                            "{'a': 4}]"),
            mock.call.info("Closing connection from 127.0.0.1 port 1234"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)
        method.assert_called_once_with(1, 2, 3, a=4)


class TestControlDaemon(unittest2.TestCase):
    def test_init(self):
        cd_rpc = remote.ControlDaemonRPC('host', 'port', 'authkey', 'daemon')

        self.assertEqual(cd_rpc.daemon, 'daemon')

    def test_get_limits(self):
        daemon = mock.Mock(limits=mock.Mock(**{
            'get_limits.return_value': 'limits',
        }))
        cd_rpc = remote.ControlDaemonRPC('host', 'port', 'authkey', daemon)
        cd_rpc.mode = 'server'

        result = cd_rpc.get_limits('sum')

        self.assertEqual(result, 'limits')
        daemon.limits.get_limits.assert_called_once_with('sum')


class TestRemoteLimitData(unittest2.TestCase):
    def test_init(self):
        rld = remote.RemoteLimitData('rpc')

        self.assertEqual(rld.limit_rpc, 'rpc')
        self.assertIsInstance(rld.limit_lock, eventlet.semaphore.Semaphore)

    def test_set_limits(self):
        rld = remote.RemoteLimitData('rpc')

        self.assertRaises(ValueError, rld.set_limits, 'limits')

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits_newlimits(self, mock_Semaphore):
        rpc = mock.Mock(**{
            'get_limits.return_value': 'new limits',
        })
        rld = remote.RemoteLimitData(rpc)

        result = rld.get_limits('sum')

        self.assertEqual(result, 'new limits')
        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(None, None, None),
        ])
        rpc.get_limits.assert_called_once_with('sum')

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits_nochange(self, mock_Semaphore):
        rpc = mock.Mock(**{
            'get_limits.side_effect': control.NoChangeException,
        })
        rld = remote.RemoteLimitData(rpc)

        self.assertRaises(control.NoChangeException, rld.get_limits, 'sum')

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(control.NoChangeException, mock.ANY, mock.ANY),
        ])
        rpc.get_limits.assert_called_once_with('sum')

    @mock.patch.object(eventlet.semaphore, 'Semaphore',
                       return_value=mock.MagicMock())
    def test_get_limits_exception(self, mock_Semaphore):
        rpc = mock.Mock(**{
            'get_limits.side_effect': test_utils.TestException,
        })
        rld = remote.RemoteLimitData(rpc)

        self.assertRaises(control.NoChangeException, rld.get_limits, 'sum')

        mock_Semaphore.return_value.assert_has_calls([
            mock.call.__enter__(),
            mock.call.__exit__(control.NoChangeException, mock.ANY, mock.ANY),
        ])
        rpc.get_limits.assert_called_once_with('sum')


class TestRemoteControlDaemon(unittest2.TestCase):
    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    def test_init_missing_host(self, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.port': '1234',
            'remote.authkey': 'authkey',
        })

        self.assertRaises(ValueError, remote.RemoteControlDaemon,
                          'middleware', conf)
        mock_warn.assert_called_once_with(
            "Missing value for configuration key 'control.remote.host'")
        self.assertFalse(mock_ControlDaemonRPC.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    def test_init_missing_port(self, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.authkey': 'authkey',
        })

        self.assertRaises(ValueError, remote.RemoteControlDaemon,
                          'middleware', conf)
        mock_warn.assert_called_once_with(
            "Missing value for configuration key 'control.remote.port'")
        self.assertFalse(mock_ControlDaemonRPC.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    def test_init_invalid_port(self, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': 'some port',
            'remote.authkey': 'authkey',
        })

        self.assertRaises(ValueError, remote.RemoteControlDaemon,
                          'middleware', conf)
        mock_warn.assert_called_once_with(
            "Invalid value for configuration key 'control.remote.port'")
        self.assertFalse(mock_ControlDaemonRPC.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    def test_init_missing_authkey(self, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': '1234',
        })

        self.assertRaises(ValueError, remote.RemoteControlDaemon,
                          'middleware', conf)
        mock_warn.assert_called_once_with(
            "Missing value for configuration key 'control.remote.authkey'")
        self.assertFalse(mock_ControlDaemonRPC.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    def test_init(self, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': '1234',
            'remote.authkey': 'authkey',
        })

        rcd = remote.RemoteControlDaemon('middleware', conf)

        self.assertEqual(rcd.middleware, 'middleware')
        self.assertEqual(rcd.config, conf)
        mock_ControlDaemonRPC.assert_called_once_with(
            daemon=rcd, host='host', port=1234, authkey='authkey')
        self.assertEqual(rcd.remote, 'rpc')
        self.assertEqual(rcd.remote_limits, None)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    @mock.patch.object(remote, 'RemoteLimitData', return_value='limits')
    def test_get_limits_set(self, mock_RemoteLimitData,
                            mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': '1234',
            'remote.authkey': 'authkey',
        })
        rcd = remote.RemoteControlDaemon('middleware', conf)
        rcd.remote_limits = 'something'

        result = rcd.get_limits()

        self.assertEqual(result, 'something')
        self.assertFalse(mock_RemoteLimitData.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    @mock.patch.object(remote, 'RemoteLimitData', return_value='limits')
    def test_get_limits_unset(self, mock_RemoteLimitData,
                              mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': '1234',
            'remote.authkey': 'authkey',
        })
        rcd = remote.RemoteControlDaemon('middleware', conf)

        result = rcd.get_limits()

        self.assertEqual(result, 'limits')
        mock_RemoteLimitData.assert_called_once_with('rpc')

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value=mock.Mock())
    @mock.patch.object(control.ControlDaemon, 'start')
    def test_serve(self, mock_start, mock_ControlDaemonRPC, mock_warn):
        conf = dict(control={
            'remote.host': 'host',
            'remote.port': '1234',
            'remote.authkey': 'authkey',
        })
        rcd = remote.RemoteControlDaemon('middleware', conf)

        rcd.serve()

        mock_start.assert_called_once_with()
        mock_ControlDaemonRPC.return_value.listen.assert_called_once_with()

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    @mock.patch.object(config.Config, 'get_database', return_value='database')
    def test_db_set(self, mock_get_database, mock_ControlDaemonRPC,
                    mock_warn):
        conf = config.Config(conf_dict={
            'control.remote.host': 'host',
            'control.remote.port': '1234',
            'control.remote.authkey': 'authkey',
        })
        rcd = remote.RemoteControlDaemon('middleware', conf)
        rcd._db = 'something'

        self.assertEqual(rcd.db, 'something')
        self.assertFalse(mock_get_database.called)

    @mock.patch('warnings.warn')
    @mock.patch.object(remote, 'ControlDaemonRPC', return_value='rpc')
    @mock.patch.object(config.Config, 'get_database', return_value='database')
    def test_db_unset(self, mock_get_database, mock_ControlDaemonRPC,
                      mock_warn):
        conf = config.Config(conf_dict={
            'control.remote.host': 'host',
            'control.remote.port': '1234',
            'control.remote.authkey': 'authkey',
        })
        rcd = remote.RemoteControlDaemon('middleware', conf)

        self.assertEqual(rcd.db, 'database')
        mock_get_database.assert_called_once_with()
