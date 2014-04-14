# Copyright 2014 Urban Airship
import time
import socket

from kiddiepool.exceptions import (
    KiddieConnectionRecvFailure, KiddieConnectionSendFailure
)


# Connection defaults
DEFAULT_MAX_IDLE = 60
# Non-None lifetime allows slow rebalancing after failures
DEFAULT_LIFETIME = 60 * 5
DEFAULT_TIMEOUT = 3  # connect() and send() timeout


class _ConnectionContext(object):
    """Context Manager to handle Connections"""
    def __init__(self, pool):
        self.conn = None
        self.pool = pool

    def __enter__(self):
        self.conn = self.pool.get()
        return self.conn

    def __exit__(self, exc_type, exc_value, exc_tb):
        # Regardless of whether or not the connection is valid, put it back
        # in the pool. The next get() will determine it's validity.
        self.pool.put(self.conn)
        if exc_type is not None:
            # Let handle_exception suppress the exception if desired
            return bool(
                self.conn.handle_exception(exc_type, exc_value, exc_tb)
            )


class KiddieConnection(object):
    """
    TCP Base Connection Class

    Features:
     * TCP Keepalives on by default
     * Configurable timeout for socket operations
     * Tracks age and idle time for pools to refresh/cull idle/old connections
    """

    SendException = KiddieConnectionSendFailure
    RecvException = KiddieConnectionRecvFailure

    def __init__(self, lifetime=DEFAULT_LIFETIME, max_idle=DEFAULT_MAX_IDLE,
                 tcp_keepalives=True, timeout=DEFAULT_TIMEOUT):
        self.host = None
        self.port = None
        self.socket = None
        self.max_idle = max_idle
        self.tcp_keepalives = tcp_keepalives
        self.timeout = timeout
        self.last_touch = 0
        # lifetime is how many seconds the connection lives
        self.lifetime = lifetime
        # endoflife is when the connection should die and be reconnected
        self.endoflife = 0

    @property
    def closed(self):
        return self.socket is None

    def connect(self, host, port):
        self.host = host
        self.port = port
        if self.socket is not None:
            self.close()
        try:
            self._open()
        except socket.error:
            return False
        else:
            return True

    def _open(self):
        self.socket = socket.create_connection(
            (self.host, self.port), timeout=self.timeout
        )
        if self.tcp_keepalives:
            self.socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
            )

        # Touch to update the idle check
        self.touch()

        # Reset endoflife
        if self.lifetime is not None:
            self.endoflife = time.time() + self.lifetime

    def touch(self):
        self.last_touch = time.time()

    def close(self):
        self.socket.close()
        self.socket = None

    def sendall(self, payload):
        try:
            self.socket.sendall(payload)
        except socket.error as e:
            raise self.SendException(e)
        self.touch()

    def recv(self, size, flags=0):
        try:
            data = self.socket.recv(size, flags)
        except socket.error as e:
            raise self.RecvException(e)
        self.touch()
        return data

    def recvall(self, size):
        """Receive `size` data and return it or raise a socket.error"""
        data = []
        received = 0
        try:
            while received < size:
                chunk = self.recv(size - received)
                if not chunk:
                    raise self.RecvException(
                        'Received %d bytes out of %d.' % (received, size)
                    )
                data.append(chunk)
                received += len(chunk)
        except socket.error as e:
            raise self.RecvException(e)
        return b"".join(data)

    def handle_exception(self, exc_type, exc_value, exc_tb):
        """Close connection on socket errors"""
        if issubclass(exc_type, socket.error):
            self.close()

    def validate(self):
        """
        Returns True if connection is still valid, otherwise False

        Takes into account socket status, idle time, and lifetime
        """
        if self.closed:
            # Invalid because it's closed
            return False

        now = time.time()
        if (now - self.last_touch) > self.max_idle:
            # Invalid because it's been idle too long
            return False

        if self.lifetime is not None and self.endoflife < now:
            # Invalid because it's outlived its lifetime
            return False

        return True
