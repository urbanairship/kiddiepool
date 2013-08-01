# Copyright 2012 Urban Airship
import collections
import Queue as queue
import random
import socket
import time

from kazoo.client import KazooClient
from kazoo.handlers.threading import TimeoutError as KazooTimeoutError


# Pool classes/defaults
CandidatePool = collections.deque
ConnectionPool = queue.LifoQueue
DEFAULT_POOL_MAX = 10
DEFAULT_POOL_TIMEOUT = 2
DEFAULT_CONNECT_ATTEMPTS = 2

# Connection defaults
DEFAULT_MAX_IDLE = 60
# Non-None lifetime allows slow rebalancing after failures
DEFAULT_LIFETIME = 60 * 5
DEFAULT_TIMEOUT = 3  # connect() and send() timeout

DEFAULT_SEND_ATTEMPTS = 2

DEFAULT_ZOOKEEPER_TIMEOUT = 10


class KiddieException(Exception):
    """Base class for Kiddie Exceptions"""


class KiddiePoolEmpty(KiddieException, queue.Empty):
    """No Kiddie connections available in pool (even after timeout)"""


class KiddiePoolMaxAttempts(KiddieException, socket.error):
    """Unable to connect to any Kiddie servers (even after timeout & retries)
    """


class KiddieSocketError(socket.error):
    """Base class for KiddieClientSend/Recv failures."""


class KiddieConnectionSendFailure(KiddieSocketError):
    """ KiddieConnection failed to send request """


class KiddieConnectionRecvFailure(KiddieSocketError):
    """ KiddieConnection failed to receive response """


class KiddieClientSendFailure(KiddieConnectionSendFailure):
    """KiddieClient failed to send request"""


class KiddieClientRecvFailure(KiddieConnectionRecvFailure):
    """KiddieClient failed to receive response"""


class KiddieZookeeperException(KiddieException):
    """TidePool failed to connect to Zookeeper"""


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
    """TCP Base Connection Class

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
        self.born = time.time()
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
        """Returns True if connection is still valid, otherwise False

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


class KiddiePool(object):
    """Lazy/dumb/resilient Connection Pool Implementation

     * Lazily connects to servers
     * Retries servers on faults
     * Rebalances by giving connections a lifetime and never culling candidate
       list (so bad servers will continue to get retried)

     `connect_attempts` is the number of times to try to connect to the
     *entire* list of hosts.
     """

    connection_factory = KiddieConnection

    def __init__(self, hosts, connect_attempts=DEFAULT_CONNECT_ATTEMPTS,
                 connection_factory=None, connection_options=None,
                 max_size=DEFAULT_POOL_MAX,
                 pool_timeout=DEFAULT_POOL_TIMEOUT):
        cleaned_hosts = []
        for host_pair in hosts:
            host, port = host_pair.split(':')
            cleaned_hosts.append((host, int(port)))
        random.shuffle(cleaned_hosts)
        self.candidate_pool = CandidatePool(cleaned_hosts)
        self.connection_pool = ConnectionPool(maxsize=max_size)
        self.pool_timeout = pool_timeout
        self.max_size = max_size
        self.full = False
        if connection_factory:
            self.connection_factory = connection_factory
        self.connection_options = connection_options or {}
        self.connect_attempts = connect_attempts

        # Pre-fill pool with unconnected clients
        for _ in range(max_size):
            kid = self.connection_factory(**self.connection_options)
            self.connection_pool.put(kid)

    def _connect(self, conn):
        """Make sure a resource is connected

        Can take up to (retries * timeout) seconds to return.
        Raises `KiddiePoolMaxTries` after exhausting retries on list of
        hosts.
        """
        # Rotate candidate pool so next connect starts on a different host
        self.candidate_pool.rotate(1)
        candidates = list(self.candidate_pool)
        for attempt in range(self.connect_attempts):
            for host, port in candidates:
                if conn.connect(host, port):
                    # Connection succeeded, return
                    return
        raise KiddiePoolMaxAttempts(
            "Failed to connect to any servers after %d attempts on %r" %
            (self.connect_attempts, candidates))

    def get(self):
        """Get a connection from the pool

        Raises `KiddiePoolEmpty` if no connections are available after
        pool_timeout. Can block up to (retries * timeout) + pool_timeout
        seconds.
        """
        try:
            conn = self.connection_pool.get(timeout=self.pool_timeout)
        except queue.Empty:
            raise KiddiePoolEmpty(
                'All %d connections checked out' % self.max_size
            )

        # If anything fails before we return the connection we have to put the
        # connection back into the pool as the caller won't have a reference to
        # it
        try:
            if not self.validate(conn):
                self._connect(conn)
        except Exception:
            # Could not connect, return connection to pool and re-raise
            self.put(conn)
            raise
        return conn

    def validate(self, conn):
        host_tuple = (conn.host, conn.port)
        return conn.validate() and host_tuple in self.candidate_pool

    def put(self, conn):
        """Put a connection back into the pool

        Returns instantly (no blocking)
        """
        try:
            self.connection_pool.put_nowait(conn)
        except queue.Full:
            # This is an overflow connection, close it
            conn.close()

    def connection(self):
        return _ConnectionContext(self)


class TidePool(object):
    """Multithreaded, ZooKeeper-bound KiddiePool wrapper.

    While context manager is active, a thread will monitor the children
    of the given znode and set them as the KiddiePool hosts whenever
    they change.

    KiddiePool hosts can also be set manually, without using Zookeeper.
    """

    def __init__(self, zk_quorum=None, znode_parent=None,
                 zk_timeout=DEFAULT_ZOOKEEPER_TIMEOUT, **kwargs):
        self.zk_quorum = zk_quorum
        self.znode_parent = znode_parent
        self.zk_timeout = zk_timeout
        self.kwargs = kwargs
        self.pool = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *args):
        return self.stop(*args)

    def start(self):
        """Build new Zookeeper session; start watching znode_parent's children.

        This method returns the pool, since that's what's really useful."""
        self._zk_session = KazooClient(
            self.zk_quorum,
            timeout=self.zk_timeout,
            read_only=True
        )

        # Start the session with Zookeeper
        try:
            self._zk_session.start(self.zk_timeout)
        except KazooTimeoutError:
            raise KiddieZookeeperException("Could not connect to zookeeper.")

        # Spawn Zookeeper monitoring thread
        self._zk_session.ChildrenWatch(self.znode_parent, func=self.set_hosts)

        # Do initial KiddiePool setup
        if not self.pool:
            self.pool = KiddiePool(self._initial_hosts, **self.kwargs)

            del self._initial_hosts

        return self.pool

    def stop(self, *args):
        """Stop Zookeeper session. Pool will remain but will not be updated."""
        self._zk_session.stop()

    def set_hosts(self, hosts):
        """Used by Zookeeper as callback. Can also be run manually."""
        if not self.pool:
            self._initial_hosts = hosts
            return

        cleaned_hosts = []
        for host_pair in hosts:
            host, port = host_pair.split(':')
            cleaned_hosts.append((host, int(port)))
        random.shuffle(cleaned_hosts)

        self.pool.candidate_pool = CandidatePool(cleaned_hosts)


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


class FakeConnection(object):
    """Connection class for testing (noops)"""
    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 9000)
        self.closed = kwargs.get('closed', False)

    def connect(self, host, port):
        self.host = host
        self.port = port
        return True

    def close(self):
        self.closed = True

    def sendall(self, payload):
        pass

    def recv(self, size, flags=0):
        return ''

    def recvall(self, size):
        return ''

    def handle_exception(self, et, ev, etb):
        if issubclass(et, socket.error):
            self.close()

    def validate(self):
        return True
