# Copyright 2012 Urban Airship
import collections
import Queue as queue
import random
import socket
import time

from threading import Lock as threading_lock

from kazoo.exceptions import NoNodeError


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


class TidePoolException(KiddieException):
    """KiddieException subclass for grouping TidePool errors."""


class TidePoolAlreadyBoundError(TidePoolException):
    """Attempted to bind a bound TidePool"""


class TidePoolAlreadyUnboundError(TidePoolException):
    """Attempted to unbind an unbound TidePool"""


class TidePoolBindError(TidePoolException):
    """Failed to bind TidePool to zk_session."""


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


class KiddiePool(object):
    """Lazy/dumb/resilient Connection Pool Implementation

     * Lazily connects to servers
     * Retries servers on faults
     * Rebalances by giving connections a lifetime and never culling candidate
       list (so bad servers will continue to get retried)
     * Host-port pairs can be reset on the fly

     `connect_attempts` is the number of times to try to connect to the
     *entire* list of hosts.
     """

    connection_factory = KiddieConnection

    def __init__(self, hosts, connect_attempts=DEFAULT_CONNECT_ATTEMPTS,
                 connection_factory=None, connection_options=None,
                 max_size=DEFAULT_POOL_MAX,
                 pool_timeout=DEFAULT_POOL_TIMEOUT):

        self.set_hosts(hosts)
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
        """
        Make sure a resource is connected

        Can take up to (retries * timeout) seconds to return.
        Raises `KiddiePoolMaxAttempts` after exhausting retries on list of
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
        """
        Get a connection from the pool

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
        """
        Return True if connection is still valid, otherwise False.

        The socket may have closed or the host-port pair become invalid.
        """
        host_tuple = (conn.host, conn.port)
        return conn.validate() and host_tuple in self.candidate_pool

    def put(self, conn):
        """
        Put a connection back into the pool

        Returns instantly (no blocking)
        """
        try:
            self.connection_pool.put_nowait(conn)
        except queue.Full:
            # This is an overflow connection, close it
            conn.close()

    def connection(self):
        return _ConnectionContext(self)

    def set_hosts(self, hosts):
        """Initialize the candidate_pool. Can be rerun on the fly."""
        cleaned_hosts = []
        for host_pair in hosts:
            host, port = host_pair.split(':')
            cleaned_hosts.append((host, int(port)))
        random.shuffle(cleaned_hosts)

        self.candidate_pool = CandidatePool(cleaned_hosts)


class TidePool(KiddiePool):
    """Multithreaded, Zookeeper-bound KiddiePool wrapper.

    While context manager is active, a thread will monitor the children
    of the given znode and set them as the KiddiePool hosts whenever
    they change.
    """

    def __init__(self, zk_session, znode_parent,
                 deferred_bind=False, lock=threading_lock, **kwargs):
        """
        Initialize KiddiePool and store relevant Zookeeper data.

        zk_session    - KazooClient session
        znode_parent  - The znode whose children contain host-port pairs
        deferred_bind - If True, don't call bind() during __init__
        **kwargs      - All other kwargs are passed to KiddiePool's __init__
        """
        super(TidePool, self).__init__([], **kwargs)

        self._zk_session = zk_session
        self._znode_parent = znode_parent

        self._bind_lock = lock()
        self._data_watcher = None
        self._child_watcher = None

        if not deferred_bind:
            self.bind()

    def __enter__(self):
        """Context manager start method -- Ensure TidePool is bound."""
        if not self._bind_lock.locked():
            self.bind()

        return self

    def __exit__(self, *args):
        """Context manager stop method -- Ensure TidePool is unbound."""
        if self._bind_lock.locked():
            self.unbind()

    def bind(self):
        """Build new Zookeeper session. Watch self._znode_parent's children."""

        if not self._bind_lock.acquire(False):
            raise TidePoolAlreadyBoundError(
                "Cannot bind previously bound TidePool instance."
            )

        # Spawn Zookeeper monitoring thread
        self._data_watcher = self._zk_session.DataWatch(
            self._znode_parent, func=self._handle_znode_parent_change
        )

        if self._data_watcher._session_watcher not in \
                self._zk_session.state_listeners:
            raise TidePoolBindError("Could not bind to Zookeeper session.")

    def unbind(self):
        """Stop Zookeeper session. Pool will no longer be updated."""

        # Completely remove the DataWatch event handler
        if self._data_watcher is not None:
            self._zk_session.remove_listener(self._data_watcher)
            self._zk_session._data_watchers[self._znode_parent].discard(
                self._data_watcher._watcher
            )
            self._data_watcher = None

        # Completely remove the ChildrenWatch event handler
        if self._child_watcher is not None:
            self._zk_session.remove_listener(self._child_watcher)
            self._zk_session._child_watchers[self._znode_parent].discard(
                self._child_watcher._watcher
            )
            self._child_watcher = None

        # Ensure the lock is released, so TidePool can be rebound
        try:
            self._bind_lock.release()
        except Exception:  # Unfortunately some locks throw different errors
            pass

    def _handle_znode_parent_change(self, data, stats):
        """Callback for znode_parent DataWatcher. Sets ChildWatcher."""
        if data is not None and \
                self._znode_parent not in self._zk_session._child_watchers:
            try:
                self._child_watcher = self._zk_session.ChildrenWatch(
                    self._znode_parent, func=self.set_hosts
                )
            except NoNodeError:
                # Node was deleted between created event receipt and request
                # We will try again when next DataWatch event happens
                pass


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
        self.tcp_keepalives = kwargs.get('tcp_keepalives', True)

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


class FakeKazooClient(object):
    class FakeWatcher(object):
        def _session_watcher(self):
            pass

        def _watcher(self):
            pass

    def __init__(self):
        self.client_state = 'CLOSED'
        self.state_listeners = set()
        self._child_watchers = collections.defaultdict(set)
        self._data_watchers = collections.defaultdict(set)

    def start(self):
        self.client_state = 'CONNECTED'

    def stop(self):
        self.client_state = 'CLOSED'

    def add_listener(self, watcher):
        self.state_listeners.add(watcher._session_watcher)

    def remove_listener(self, watcher):
        self.state_listeners.discard(watcher._session_watcher)

    def DataWatch(self, znode, func=None):
        data_watcher = self.FakeWatcher()
        self._data_watchers[znode].add(data_watcher._watcher)
        self.add_listener(data_watcher)
        return data_watcher

    def ChildrenWatch(self, znode, func=None):
        child_watcher = self.FakeWatcher()
        self._child_watchers[znode].add(child_watcher._watcher)
        self.add_listener(child_watcher)
        return child_watcher
