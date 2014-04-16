import os
import time
import sys

from kazoo.testing import harness
from nose.tools import make_decorator
import kiddiepool


def check_env(func):
    @make_decorator(func)
    def f(*args):
        if os.getenv('ZOOKEEPER_PATH') is None:
            print >> sys.stderr, "ZOOKEEPER_PATH not set; skipping integration test"
            return

        return func(*args)

    return f

class BasicPoolBehavior(harness.KazooTestHarness):
    SERVICE_PATH = '/services/herpderp/0.1'
    FAKE_HOST = '127.0.254.1:12345'

    @check_env
    def setUp(self):
        self.setup_zookeeper()
        self.client.ensure_path(self.SERVICE_PATH)
        self.pool = kiddiepool.TidePool(self.client, self.SERVICE_PATH)

    @check_env
    def tearDown(self):
        self.teardown_zookeeper()

    @check_env
    def test_pool_mutation(self):
        assert len(self.pool.candidate_pool) == 0
        self.client.create('{0}/{1}'.format(self.SERVICE_PATH, self.FAKE_HOST))
        # Concurrency is hard. Let's take a nap!
        time.sleep(0.5)
        assert len(self.pool.candidate_pool) == 1
        host, port = self.FAKE_HOST.split(':')
        host_tuple = (unicode(host), int(port))
        assert host_tuple in self.pool.candidate_pool
