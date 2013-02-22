import os
import shutil
import tempfile
from datetime import datetime

from disco.worker.classic.worker import Params

from nose.tools import assert_raises
from nose.tools import raises
from nose.tools import eq_
from nose.tools import ok_
from inferno.lib.datefile import Datefile

from inferno.lib.job import InfernoJob
from inferno.lib import pid
from inferno.lib.rule import InfernoRule
from inferno.lib.settings import InfernoSettings


class TestDaemonPid(object):

    def setUp(self):
        self.settings = InfernoSettings()
        self._make_temp_pid_dir()
        self.job = InfernoJob(InfernoRule(name='some_rule_name'), {}, Params())
        self.pid_dir = pid.pid_dir(self.settings)

    def test_create_pid(self):
        # create the pid file
        eq_(pid.create_pid(self.pid_dir, self.job.rule, str(os.getpid())),
            True)

    @raises(IOError)
    def test_create_pid_exception(self):
        unknown_path = os.path.join('some', 'unknown', 'path')
        pid_dir = self.settings['pid_dir'] = unknown_path
        pid.create_pid(pid_dir, self.job.rule, str(os.getpid()))

    def test_remove_pid(self):
        # create the pid file
        eq_(pid.create_pid(self.pid_dir, self.job.rule, str(os.getpid())),
            True)
        # remove the pid file
        pid.remove_pid(self.pid_dir, self.job.rule)
        # can't remove it again
        assert_raises(OSError, pid.remove_pid, self.pid_dir, self.job.rule)

    def test_processes(self):
        eq_(pid.processes(self.pid_dir), [])
        eq_(pid.create_pid(self.pid_dir, self.job.rule, str(os.getpid())),
            True)
        actual = pid.processes(self.pid_dir)
        eq_(len(actual), 1)
        eq_(set(actual[0].keys()), set(['pid', 'timestamp', 'name']))
        eq_(actual[0]['name'], 'some_rule_name')
        ok_(int(actual[0]['pid']) > 0)
        eq_(int(actual[0]['pid']), os.getpid())

    def test_should_run(self):
        # without last run file

        eq_(pid.should_run(self.pid_dir, self.job.rule), True)

        print 'hee --> %s %s' % (self.pid_dir,self.job.rule_name)

        self._make_temp_pid_file()
        # with last run file that's new
        d = Datefile(self.pid_dir, "%s.last_run" % self.job.rule_name,
                 timestamp=datetime.utcnow())
        print 'yo --> %s' % d.timestamp
        eq_(pid.should_run(self.pid_dir, self.job.rule), False)

        # with last run file that's old
        Datefile(self.pid_dir, "%s.last_run" % self.job.rule_name,
            timestamp=Datefile.EPOCH)
        eq_(pid.should_run(self.pid_dir, self.job.rule), False)

        os.remove('%s/%s.pid' % (self.pid_dir, self.job.rule_name))

        # right date, with no pid
        Datefile(self.pid_dir, "%s.last_run" % self.job.rule_name,
                 timestamp=Datefile.EPOCH)
        eq_(pid.should_run(self.pid_dir, self.job.rule), True)


    def _make_temp_pid_dir(self):
        temp_dir = tempfile.gettempdir()
        self.settings['pid_dir'] = os.path.join(temp_dir, 'inferno')
        pid_dir = self.settings['pid_dir']
        if os.path.exists(pid_dir):
            shutil.rmtree(pid_dir)

    def _make_temp_pid_file(self):
        f = open('%s/%s.pid' % (self.pid_dir, self.job.rule_name), 'w')

