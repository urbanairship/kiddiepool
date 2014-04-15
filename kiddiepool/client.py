import socket
from kiddiepool.exceptions import (
    KiddieClientSendFailure, KiddieClientRecvFailure
)

DEFAULT_SEND_ATTEMPTS = 2


class KiddieClient(object):
    """Thread-safe wrapper around a KiddiePool of KiddieConnections

    Supports multiple connection attempts
    """

    SendException = KiddieClientSendFailure
    RecvException = KiddieClientRecvFailure

    def __init__(self, pool, send_attempts=DEFAULT_SEND_ATTEMPTS):
        self.pool = pool
        self.send_attempts = send_attempts

    def _recv(self, size):
        """Recv -- No retry logic because that doesn't usually make sense"""
        try:
            with self.pool.connection() as conn:
                return conn.recvall(size)
        except socket.error as e:
            raise self.RecvException(
                'Failed to recv %s bytes. Last exception: %r ' % (size, e)
            )

    def _sendall(self, request, attempts=None):
        """Fire-and-forget with configurable retries"""
        e = None
        if attempts is None:
            attempts = self.send_attempts

        for attempt in range(attempts):
            try:
                with self.pool.connection() as conn:
                    conn.sendall(request)
            except socket.error as e:
                continue
            else:
                break
        else:
            # for-loop exited meaning attempts were exhausted
            raise self.SendException(
                'Failed to send request (%d bytes) after %d attempts. '
                'Last exception: %r' % (len(request), attempts, e)
            )
