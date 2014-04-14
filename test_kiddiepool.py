import time
import socket

import mimic

from kiddiepool import KiddieConnection, KiddiePool, TidePool
from kiddiepool.fake import FakeConnection, FakeKazooClient
from kiddiepool.exceptions import (
    KiddieConnectionRecvFailure, KiddiePoolEmpty, KiddiePoolMaxAttempts
)

from kazoo.exceptions import NoNodeError


class TestKiddieConnection(mimic.MimicTestBase):
    def setUp(self):
        super(TestKiddieConnection, self).setUp()
        self.conn = KiddieConnection()

    def test_simple_recvall(self):
        self.mimic.stub_out_with_mock(self.conn, 'recv')
        self.conn.recv(3).and_return('123')

        self.mimic.replay_all()

        data = self.conn.recvall(3)
        self.assertEqual('123', data)

    def test_multi_read_recvall(self):
        self.mimic.stub_out_with_mock(self.conn, 'recv')
        self.conn.recv(10).and_return('123')
        self.conn.recv(7).and_return('456')
        self.conn.recv(4).and_return('789')
        self.conn.recv(1).and_return('0')

        self.mimic.replay_all()

        data = self.conn.recvall(10)
        self.assertEqual('1234567890', data)

    def test_failed_recvall(self):
        self.mimic.stub_out_with_mock(self.conn, 'recv')
        self.conn.recv(10).and_return('123')
        self.conn.recv(7).and_return('456')
        self.conn.recv(4).and_return('789')
        self.conn.recv(1).and_return('')

        self.mimic.replay_all()

        self.assertRaises(
            KiddieConnectionRecvFailure,
            self.conn.recvall,
            10
        )

    def test_broken_pipe(self):
        self.mimic.stub_out_with_mock(self.conn, 'recv')
        self.conn.recv(10)\
            .and_raise(socket.error(socket.errno.EPIPE, 'Broken pipe'))

        self.mimic.replay_all()

        self.assertRaises(
            KiddieConnectionRecvFailure,
            self.conn.recvall,
            10
        )

    def test_socket_error_conversion_to_kiddiepool_socket_error(self):
        arbitrary_size = 10
        arbitrary_flags = 0

        self.conn.socket = self.mimic.create_mock_anything()
        self.conn.socket.recv(arbitrary_size, arbitrary_flags).and_raise(
            socket.error
        )

        self.mimic.replay_all()

        self.assertRaises(
            KiddieConnectionRecvFailure,
            self.conn.recv,
            arbitrary_size,
            arbitrary_flags,
        )

    def test_connection_valid(self):
        mock_socket = self.mimic.create_mock_anything()
        mock_socket.setsockopt(
            mimic.IgnoreArg(), mimic.IgnoreArg(), mimic.IgnoreArg()
        )
        self.mimic.stub_out_with_mock(socket, 'create_connection')
        socket.create_connection(
            ('lol', 643), timeout=mimic.IgnoreArg()
        ).AndReturn(mock_socket)

        # Set max idle time to absurd number and remove lifetime, if any
        self.conn = KiddieConnection(max_idle=999, lifetime=None)

        self.mimic.replay_all()

        self.conn.connect('lol', 643)

        # Make sure connection is valid
        self.assertTrue(self.conn.validate())

    def test_max_idle(self):
        mock_socket = self.mimic.create_mock_anything()
        mock_socket.setsockopt(
            mimic.IgnoreArg(), mimic.IgnoreArg(), mimic.IgnoreArg()
        )
        self.mimic.stub_out_with_mock(socket, 'create_connection')
        socket.create_connection(
            ('foo', 123), timeout=mimic.IgnoreArg()
        ).AndReturn(mock_socket)

        # Set max idle time to 0 and remove lifetime, if any
        self.conn = KiddieConnection(max_idle=0, lifetime=None)

        self.mimic.replay_all()

        self.conn.connect('foo', 123)

        # Make sure we invalidate a connection immediately
        self.assertFalse(self.conn.validate())

    def test_connection_end_of_life(self):
        mock_socket = self.mimic.create_mock_anything()
        mock_socket.setsockopt(
            mimic.IgnoreArg(), mimic.IgnoreArg(), mimic.IgnoreArg()
        )
        self.mimic.stub_out_with_mock(socket, 'create_connection')
        socket.create_connection(
            ('bar', 321), timeout=mimic.IgnoreArg()
        ).AndReturn(mock_socket)

        # Set lifetime to 0 and make max_idle absurdly large
        self.conn = KiddieConnection(max_idle=999, lifetime=0)

        self.mimic.replay_all()

        self.conn.connect('bar', 321)

        # Make sure we invalidate a connection immediately
        self.assertFalse(self.conn.validate())

    def test_timeout(self):
        mock_socket = self.mimic.create_mock_anything()
        mock_socket.setsockopt(
            mimic.IgnoreArg(), mimic.IgnoreArg(), mimic.IgnoreArg()
        )
        self.mimic.stub_out_with_mock(socket, 'create_connection')

        # Set expectation that 987 is passed in at socket creation time
        socket.create_connection(
            ('baz', 222), timeout=987
        ).AndReturn(mock_socket)

        # Set timeout to 987 in instantiation of KiddieConnection
        self.conn = KiddieConnection(timeout=987)

        self.mimic.replay_all()

        self.conn.connect('baz', 222)


class TestKiddiePool(mimic.MimicTestBase):
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

        # Make sure it tries each host right number of times
        self.mimic.stub_out_with_mock(conn, 'connect')
        conn.connect('foo', 123).InAnyOrder().AndReturn(False)
        conn.connect('foo', 123).InAnyOrder().AndReturn(False)
        conn.connect('bar', 321).InAnyOrder().AndReturn(False)
        conn.connect('bar', 321).InAnyOrder().AndReturn(False)

        self.mimic.replay_all()

        # Make sure it raises KiddiePoolMaxAttempts as well
        self.assertRaises(
            KiddiePoolMaxAttempts,
            self.pool._connect,
            conn
        )

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


class TestTidePool(mimic.MimicTestBase):
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
        self.mimic.replay_all()

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
        self.mimic.stub_out_with_mock(self.zk_session, 'ChildrenWatch')

        self.zk_session.ChildrenWatch(
            self.tide_pool._znode_parent,
            func=self.tide_pool.set_hosts
        ).AndRaise(NoNodeError)

        self.mimic.replay_all()

        self.tide_pool._handle_znode_parent_change('herp,derp', {})

        # Implicit assertion is that NoNodeError is swallowed
