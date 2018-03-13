import os
import time

from kazoo.testing import harness

from unittest import skipIf
import kiddiepool


@skipIf(
    not os.getenv('ZOOKEEPER_PATH'),
    "ZOOKEEPER_PATH is not defined; skipping integration tests"
)
class BasicPoolBehavior(harness.KazooTestHarness):
    SERVICE_PATH = '/services/herpderp/0.1'
    FAKE_HOST = '127.0.254.1:12345'

    def setUp(self):
        self.setup_zookeeper()
        self.client.ensure_path(self.SERVICE_PATH)
        self.pool = kiddiepool.TidePool(self.client, self.SERVICE_PATH)

    def tearDown(self):
        self.teardown_zookeeper()

    def test_pool_mutation(self):
        assert len(self.pool.candidate_pool) == 0
        self.client.create('{0}/{1}'.format(self.SERVICE_PATH, self.FAKE_HOST))
        # Concurrency is hard. Let's take a nap!
        time.sleep(0.5)
        assert len(self.pool.candidate_pool) == 1
        host, port = self.FAKE_HOST.split(':')
        host_tuple = (str(host), int(port))
        assert host_tuple in self.pool.candidate_pool
