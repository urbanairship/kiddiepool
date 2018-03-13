try:
    import queue as queue
except ImportError:
    # python2
    import Queue as queue
import socket


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
