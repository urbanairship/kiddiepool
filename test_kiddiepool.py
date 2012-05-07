import socket

import mox

import kiddiepool


class TestKiddieConnection(mox.MoxTestBase):
    def setUp(self):
        super(TestKiddieConnection, self).setUp()
        self.conn = kiddiepool.KiddieConnection()
        self.mox.StubOutWithMock(self.conn, 'recv')

    def test_simple_recvall(self):
        self.conn.recv(3).AndReturn('123')

        self.mox.ReplayAll()

        data = self.conn.recvall(3)
        self.assertEqual('123', data)

    def test_multi_read_recvall(self):
        self.conn.recv(10).AndReturn('123')
        self.conn.recv(7).AndReturn('456')
        self.conn.recv(4).AndReturn('789')
        self.conn.recv(1).AndReturn('0')

        self.mox.ReplayAll()

        data = self.conn.recvall(10)
        self.assertEqual('1234567890', data)

    def test_failed_recvall(self):
        self.conn.recv(10).AndReturn('123')
        self.conn.recv(7).AndReturn('456')
        self.conn.recv(4).AndReturn('789')
        self.conn.recv(1).AndReturn('')

        self.mox.ReplayAll()

        self.assertRaises(
                kiddiepool.KiddieClientRecvFailure, self.conn.recvall, 10)

    def test_broken_pipe(self):
        self.conn.recv(10)\
                .AndRaise(socket.error(socket.errno.EPIPE, 'Broken pipe'))
        self.mox.ReplayAll()

        self.assertRaises(socket.error, self.conn.recvall, 10)
