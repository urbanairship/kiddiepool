Kiddie Pool - Python Pooling Driver Framework
=============================================

© 2013 Urban Airship

https://github.com/urbanairship/kiddiepool


.. image:: https://secure.travis-ci.org/urbanairship/kiddiepool.png?branch=master
   :target: http://travis-ci.org/urbanairship/kiddiepool/

Goals
-----

* Create a reusable connection pool class that handles failover
* Allow connection pool to discover service locations via Zookeeper

Creating a New Client
---------------------

Subclass at least ``KiddieClient`` to create the public API for your client
driver.  Should call ``KiddieClient._sendall(<str>)`` to send data.

Using the Pool
--------------

Using a static pool
~~~~~~~~~~~~~~~~~~~

#. Create a list of ``"<host>:<port>"`` strings to target for connections.
#. Instantiate a ``KiddiePool`` with that list of strings.
#. Pass the KiddiePool instance to your ``KiddieClient`` subclass for use.
#. Use your client's API and it will use the pool automatically.


Using a dynamic pool
~~~~~~~~~~~~~~~~~~~~

#. Instantiate a TidePool with the Zookeeper quorum and znode whose children
   to monitor.
#. Use a context manager ``with TidePool() as pool:`` or the ``start()`` and
   ``stop()`` methods to manage the connection to Zookeeper.
#. Use the dynamic pool exactly like the static pool.  Candidates will be
   added/removed from the pool by a background thread.
