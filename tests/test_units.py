from mock import patch, call, Mock
import socket
import time
import unittest

from kiddiepool import KiddieConnection, KiddiePool, TidePool
from kiddiepool.fake import FakeConnection, FakeKazooClient
from kiddiepool.exceptions import (
    KiddieConnectionRecvFailure, KiddiePoolEmpty, KiddiePoolMaxAttempts
)

from kazoo.exceptions import NoNodeError


class TestKiddieConnection(unittest.TestCase):
    def setUp(self):
        super(TestKiddieConnection, self).setUp()
        self.conn = KiddieConnection()

    def _patch_recv(self):
        recv_patch = patch.object(self.conn, 'recv')
        self.addCleanup(recv_patch.stop)

        return recv_patch.start()

    def test_simple_recvall(self):
        recv_mock = self._patch_recv()
        recv_mock.return_value = b'123'

        data = self.conn.recvall(3)
        self.assertEqual(b'123', data)
        recv_mock.assert_called_with(3)

    def test_multi_read_recvall(self):
        recv_mock = self._patch_recv()
        recv_mock.side_effect = [
            b'123',
            b'456',
            b'789',
            b'0',
        ]

        data = self.conn.recvall(10)
        self.assertEqual(b'1234567890', data)
        self.assertEqual(recv_mock.call_args_list,
                         [call(10), call(7), call(4), call(1)])

    def test_failed_recvall(self):
        recv_mock = self._patch_recv()
        recv_mock.side_effect = [
            b'123',
            b'456',
            b'789',
            b'',
        ]

        with self.assertRaises(KiddieConnectionRecvFailure):
            self.conn.recvall(10)

    def test_broken_pipe(self):
        recv_mock = self._patch_recv()
        recv_mock.side_effect = socket.error(socket.errno.EPIPE,
                                             'Broken pipe')

        with self.assertRaises(KiddieConnectionRecvFailure):
            self.conn.recvall(10)

    def test_socket_error_conversion_to_kiddiepool_socket_error(self):
        arbitrary_size = 10
        arbitrary_flags = 0

        with patch.object(self.conn, 'socket') as socket_mock:
            socket_mock.recv = Mock(side_effect=socket.error)

            with self.assertRaises(KiddieConnectionRecvFailure):
                self.conn.recv(arbitrary_size, arbitrary_flags)

    @patch.object(socket, 'create_connection')
    def test_connection_valid(self, mock_conn):
        # Set max idle time to absurd number and remove lifetime, if any
        self.conn = KiddieConnection(max_idle=999, lifetime=None)

        self.conn.connect('lol', 643)

        # Make sure connection is valid
        self.assertTrue(self.conn.validate())

        self.assertEqual(mock_conn.call_count, 1)
        args, kwargs = mock_conn.call_args
        self.assertEqual(args, (('lol', 643),))

    @patch.object(socket, 'create_connection')
    def test_max_idle(self, _):
        # Set max idle time to 0 and remove lifetime, if any
        self.conn = KiddieConnection(max_idle=0, lifetime=None)

        self.conn.connect('foo', 123)

        # Make sure we invalidate a connection immediately
        self.assertFalse(self.conn.validate())

    @patch.object(socket, 'create_connection')
    def test_connection_end_of_life(self, _):
        # Set lifetime to 0 and make max_idle absurdly large
        self.conn = KiddieConnection(max_idle=999, lifetime=0)

        self.conn.connect('bar', 321)

        # Make sure we invalidate a connection immediately
        self.assertFalse(self.conn.validate())

    @patch.object(socket, 'create_connection')
    def test_timeout(self, mock_conn):
        # Set timeout to 987 in instantiation of KiddieConnection
        self.conn = KiddieConnection(timeout=987)

        self.conn.connect('baz', 222)

        self.assertEqual(mock_conn.call_count, 1)
        self.assertEqual(mock_conn.call_args, call(('baz', 222), timeout=987))


class TestKiddiePool(unittest.TestCase):
    def setUp(self):
        super(TestKiddiePool, self).setUp()
        self.pool = KiddiePool(
            ['foo:123', 'bar:321'],
            connection_factory=FakeConnection,
            connection_options={'tcp_keepalives': False},
            max_size=2,
            pool_timeout=.1,
            connect_attempts=2,
        )

    def test_max_size_and_pool_timeout(self):
        # Try to get more connections than max_size
        self.pool.get()
        self.pool.get()

        # Start timer
        start = time.time()

        # Make sure getting next connection throws KiddiePoolEmpty
        self.assertRaises(
            KiddiePoolEmpty,
            self.pool.get
        )

        # Make sure it took at least the timeout we set seconds
        self.assertTrue(time.time() - start > self.pool.pool_timeout)

    def test_connect_attempts(self):
        # Make a kiddiepool and mock the connection
        conn = FakeConnection()

        with patch.object(conn, 'connect') as mock_connect:
            mock_connect.return_value = False

            with self.assertRaises(KiddiePoolMaxAttempts):
                self.pool._connect(conn)

            self.assertEqual(sorted(mock_connect.call_args_list), sorted([
                call('foo', 123),
                call('foo', 123),
                call('bar', 321),
                call('bar', 321),
            ]))

    def test_connection_options(self):
        # Pass connection options into kiddiepool (in setUp)
        # Make sure they are applied to internal connections
        self.assertFalse(self.pool.connection_pool.get().tcp_keepalives)

    def test_reset_hosts(self):
        # Get connections
        orig_conn1 = self.pool.get()
        orig_conn2 = self.pool.get()

        self.pool.put(orig_conn1)
        self.pool.put(orig_conn2)

        # Change hosts
        self.pool.set_hosts(['baz:666'])

        # Get connections
        conn1 = self.pool.get()
        conn2 = self.pool.get()

        # make sure they changed to the new hosts
        self.assertEqual(conn1.host, 'baz')
        self.assertEqual(conn2.host, 'baz')
        self.assertEqual(conn1.port, 666)
        self.assertEqual(conn2.port, 666)


class TestTidePool(unittest.TestCase):
    """Don't test kazoo. Test the implementation of kazoo, though."""

    def setUp(self):
        super(TestTidePool, self).setUp()
        self.zk_session = FakeKazooClient()
        self.zk_session.start()
        self.tide_pool = TidePool(
            self.zk_session, 'bar', deferred_bind=True,
            connection_factory=FakeConnection
        )

    def test_bind_calls_DataWatch(self):
        self.tide_pool.bind()

        self.assertTrue(
            self.tide_pool._data_watcher._watcher in
            self.zk_session._data_watchers['bar']
        )
        self.assertTrue(
            self.tide_pool._data_watcher._session_watcher in
            self.zk_session.state_listeners
        )

        self.tide_pool.unbind()

        self.assertTrue(
            self.tide_pool._data_watcher not in
            self.zk_session._data_watchers['bar']
        )

    def test_handle_znode_parent_change_calls_ChildrenWatch(self):
        # Stub out KazooClient
        with patch.object(self.zk_session, 'ChildrenWatch') as mock_watch:
            mock_watch.side_effect = NoNodeError

            self.tide_pool._handle_znode_parent_change('herp,derp', {})

            mock_watch.assert_called_with(self.tide_pool._znode_parent,
                                          func=self.tide_pool.set_hosts)

            # Implicit assertion is that NoNodeError is swallowed
