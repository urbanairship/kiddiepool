import random
try:
    import queue as queue
except ImportError:
    # python2
    import Queue as queue
import collections
from threading import Lock as threading_lock

from kiddiepool.connection import KiddieConnection, _ConnectionContext
from kiddiepool.exceptions import (
    KiddiePoolEmpty, KiddiePoolMaxAttempts,
    TidePoolBindError, TidePoolAlreadyBoundError
)

# Pool classes/defaults
CandidatePool = collections.deque
ConnectionPool = queue.LifoQueue
DEFAULT_POOL_MAX = 10
DEFAULT_POOL_TIMEOUT = 2
DEFAULT_CONNECT_ATTEMPTS = 2

DEFAULT_ZOOKEEPER_TIMEOUT = 10


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

        # ensure kazoo is available on init; this prevents us from erroring
        # later in the process, and should catch any folks who want zookeeper
        # support, but neglected to install kiddiepool properly for zookeeper
        try:
            import kazoo  # noqa
        except ImportError:
            raise ImportError(
                'unable to import `kazoo` module; ensure you have installed '
                'kiddiepool with optional zookeeper support')

        self._zk_session = zk_session
        self._znode_parent = znode_parent

        self._bind_lock = lock()
        self._data_watcher = None
        self._child_watcher = None

        if not deferred_bind:
            self.bind()

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

        if (self._data_watcher._session_watcher not in
                self._zk_session.state_listeners):
            raise TidePoolBindError("Could not bind to Zookeeper session.")

    def unbind(self):
        """Stop Zookeeper session. Pool will no longer be updated."""

        # Completely remove the DataWatch event handler
        if self._data_watcher:
            self._zk_session.remove_listener(self._data_watcher)
            self._zk_session._data_watchers[self._znode_parent].discard(
                self._data_watcher._watcher
            )
            self._data_watcher = None

        # Completely remove the ChildrenWatch event handler
        if self._child_watcher:
            self._zk_session.remove_listener(self._child_watcher)
            self._zk_session._child_watchers[self._znode_parent].discard(
                self._child_watcher._watcher
            )
            self._child_watcher = None

        # Release the lock, so TidePool can be rebound
        self._bind_lock.release()

    def _handle_znode_parent_change(self, data, stats):
        """Callback for znode_parent DataWatcher. Sets ChildWatcher."""
        from kazoo.exceptions import NoNodeError

        if (data is not None and
                self._znode_parent not in self._zk_session._child_watchers):
            try:
                self._child_watcher = self._zk_session.ChildrenWatch(
                    self._znode_parent, func=self.set_hosts
                )
            except NoNodeError:
                # Node was deleted between created event receipt and request
                # We will try again when next DataWatch event happens
                pass
