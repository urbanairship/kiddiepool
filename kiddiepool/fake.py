import collections
import socket


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
        return b''

    def recvall(self, size):
        return b''

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

    def __init__(self, *args, **kwargs):
        self.client_state = 'CLOSED'
        self.state_listeners = set()
        self._child_watchers = collections.defaultdict(set)
        self._data_watchers = collections.defaultdict(set)

    def start(self, timeout=15):
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
