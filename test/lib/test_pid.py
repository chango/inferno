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
from inferno.lib.pid import DaemonPid
from inferno.lib.rule import InfernoRule
from inferno.lib.settings import InfernoSettings


class TestDaemonPid(object):

    def setUp(self):
        self.settings = InfernoSettings()
        self._make_temp_pid_dir()
        self.job = InfernoJob(InfernoRule(name='some_rule_name'), {}, Params())
        self.pid = DaemonPid(self.settings)

    def test_create_pid(self):
        # create the pid file
        eq_(self.pid.create_pid(self.job), True)
        # can't create it again
        eq_(self.pid.create_pid(self.job), False)

    @raises(OSError)
    def test_create_pid_exception(self):
        unknown_path = os.path.join('some', 'unknown', 'path')
        self.pid._settings['pid_dir'] = unknown_path
        self.pid.create_pid(self.job)

    def test_remove_pid(self):
        # create the pid file
        eq_(self.pid.create_pid(self.job), True)
        # remove the pid file
        self.pid.remove_pid(self.job)
        # can't remove it again
        assert_raises(OSError, self.pid.remove_pid, self.job)

    def test_processes(self):
        eq_(self.pid.processes, [])
        eq_(self.pid.create_pid(self.job), True)
        actual = self.pid.processes
        eq_(len(actual), 1)
        eq_(set(actual[0].keys()), set(['pid', 'timestamp', 'name']))
        eq_(actual[0]['name'], 'some_rule_name')
        ok_(int(actual[0]['pid']) > 0)
        eq_(int(actual[0]['pid']), os.getpid())

    def test_should_run(self):
        # without last run file

        eq_(self.pid.should_run(self.job), True)

        print 'hee --> %s %s' % (self.pid._pid_dir,self.job.rule_name)

        self._make_temp_pid_file()
        # with last run file that's new
        d = Datefile(self.pid._pid_dir, "%s.last_run" % self.job.rule_name,
                 timestamp=datetime.utcnow())
        print 'yo --> %s' % d.timestamp
        eq_(self.pid.should_run(self.job), False)

        # with last run file that's old
        Datefile(self.pid._pid_dir, "%s.last_run" % self.job.rule_name,
            timestamp=Datefile.EPOCH)
        eq_(self.pid.should_run(self.job), False)

        os.remove('%s/%s.pid' % (self.pid._pid_dir, self.job.rule_name))

        # right date, with no pid
        Datefile(self.pid._pid_dir, "%s.last_run" % self.job.rule_name,
                 timestamp=Datefile.EPOCH)
        eq_(self.pid.should_run(self.job), True)


    def _make_temp_pid_dir(self):
        temp_dir = tempfile.gettempdir()
        self.settings['pid_dir'] = os.path.join(temp_dir, 'inferno')
        pid_dir = self.settings['pid_dir']
        if os.path.exists(pid_dir):
            shutil.rmtree(pid_dir)

    def _make_temp_pid_file(self):
        f = open('%s/%s.pid' % (self.pid._pid_dir, self.job.rule_name), 'w')

