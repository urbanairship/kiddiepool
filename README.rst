Kiddie Pool - Python Pooling Driver Framework
=============================================

© 2012 Urban Airship

https://github.com/urbanairship/kiddiepool

Goals
-----

* Create a reusable connection pool class that handles failover

Creating a New Client
---------------------

Subclass at least ``KiddieClient`` to create the public API for your client
driver.  Should call ``KiddieClient._sendall(<str>)`` to send data.

*TODO* Support ``recv()`` ing data too

*TODO* Have the client create it's own pool?

Using the Pool
--------------

1. Create a ``KiddiePool`` of connections (``KiddieConnections``)
2. Pass the pool instance to your ``KiddieClient`` subclass for use
3. Use your client's API and it will use the pool automatically
