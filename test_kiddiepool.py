import socket

import mimic

import kiddiepool


class TestKiddieConnection(mimic.MimicTestBase):
    def setUp(self):
        super(TestKiddieConnection, self).setUp()
        self.conn = kiddiepool.KiddieConnection()
        self.mimic.stub_out_with_mock(self.conn, 'recv')

    def test_simple_recvall(self):
        self.conn.recv(3).and_return('123')

        self.mimic.replay_all()

        data = self.conn.recvall(3)
        self.assertEqual('123', data)

    def test_multi_read_recvall(self):
        self.conn.recv(10).and_return('123')
        self.conn.recv(7).and_return('456')
        self.conn.recv(4).and_return('789')
        self.conn.recv(1).and_return('0')

        self.mimic.replay_all()

        data = self.conn.recvall(10)
        self.assertEqual('1234567890', data)

    def test_failed_recvall(self):
        self.conn.recv(10).and_return('123')
        self.conn.recv(7).and_return('456')
        self.conn.recv(4).and_return('789')
        self.conn.recv(1).and_return('')

        self.mimic.replay_all()

        self.assertRaises(
            kiddiepool.KiddieConnectionRecvFailure,
            self.conn.recvall,
            10
        )

    def test_broken_pipe(self):
        self.conn.recv(10)\
                .and_raise(socket.error(socket.errno.EPIPE, 'Broken pipe'))
        self.mimic.replay_all()

        self.assertRaises(
            kiddiepool.KiddieConnectionRecvFailure,
            self.conn.recvall,
            10
        )
