from nose.tools import ok_
import os
from inferno.lib.datefile import Datefile
from datetime import datetime
import time

TEST_FILE = 'xxxx___test_datefile___'

class TestDatefile(object):
    def delete(self):
        print 'setup'
        if os.path.exists(os.path.join('/tmp', TEST_FILE)):
            os.remove(os.path.join('/tmp', TEST_FILE))

    def test_creation(self):
        self.delete()
        before = datetime.utcnow()
        datefile = Datefile('/tmp', TEST_FILE)
        actual = datefile.timestamp
        ok_(before < actual)
        ok_(os.path.exists(os.path.join('/tmp', TEST_FILE)))

    def test_reopening(self):
        after = datetime.utcnow()
        datefile = Datefile('/tmp', TEST_FILE)
        actual = datefile.timestamp
        ok_(after > actual)

    def test_is_older_than(self):
        datefile = Datefile('/tmp', TEST_FILE)
        time.sleep(2)
        ok_(datefile.is_older_than({'seconds': 2}))
        ok_(not datefile.is_older_than({'minutes': 5}))

    def test_touch(self):
        datefile = Datefile('/tmp', TEST_FILE)
        actual = datefile.timestamp
        after = datetime.utcnow()
        datefile.touch()
        ok_(datefile.timestamp > actual)
        ok_(actual < after)




