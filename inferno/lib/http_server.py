import httplib
import logging
import os
import StringIO

import tornado.web
import ujson
import yaml

from setproctitle import setproctitle
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

import inferno.lib.daemon


log = logging.getLogger(__name__)


class RequestHandler(tornado.web.RequestHandler):

    def get_error_html(self, status_code, **kwargs):
        return {
            "code": status_code,
            "message": kwargs.get('message') or httplib.responses[status_code],
            }

    def build_url(self, *args, **kwargs):
        url = "%s://%s" % (self.request.protocol, self.request.host)
        path = self.request.path

        if kwargs.get('relative'):
            if self.request.path.endswith('.json'):
                path = self.request.path[0:-5]
            url = "%s%s" % (url, path)

        for arg in args:
            url += '/' + arg

        if kwargs.get('json', True):
            return "%s.json" % url
        else:
            return url

    def request_app(self, msg, *args):
        if getattr(self.application, 'unit_testing', False):
            func = getattr(self.application.disco_ball, msg)
            return func(*args)
        try:
            reply = None
            pipe = self.application.pipe
            try:
                pipe.send((msg, args))
            except Exception as e:
                self.send_error("Comm. error: %s" % e)
            else:
                reply = pipe.recv()
            return reply
        except Exception as e:
            self.send_error('Error receiving message: %s' % e)
            return None

    def get_jobinfo(self, job_id):
        try:
            return self.request_app('get_jobinfo', job_id)
        except Exception:
            return None


class MainHandler(RequestHandler):

    def get(self, *args, **kwargs):
        self.write(dict(
            rules=self.build_url('rules', relative=False),
            jobs=self.build_url('jobs', relative=False),
        ))


class ChildUpdateHandler(RequestHandler):

    def post(self, *args, **kwargs):
        msg = self.request.arguments['msg'][0]
        job = ujson.loads(msg)
        job_id = args[0]
        self.request_app('set_history', job_id, job)


class RuleListHandler(RequestHandler):

    def get(self, *args, **kwargs):
        mods = self.request_app('get_modules')
        rval = {}
        for mod_name, rules in mods.iteritems():
            urls = [self.build_url(mod_name, rule, relative=True)
                    for rule in rules]
            rval[mod_name] = urls
        self.write(rval)


class ModuleHandler(RequestHandler):

    def get(self, *args, **kwargs):
        module = args[0]
        rule_names = self.request_app('get_rules', module)
        if rule_names:
            urls = [self.build_url(name, relative=True) for name in rule_names]
            self.write({module: urls})
        else:
            self.send_error(message='could not find rules')


class RuleHandler(RequestHandler):

    def get(self, *args, **kwargs):
        summary = self.request_app('get_rule_summary', *args)
        if summary:
            self.write(summary)
        else:
            self.send_error(message='could not find rule')

    def post(self, *args, **kwargs):
        params = {}
        for _, ymls in self.request.arguments.iteritems():
            for yml in ymls:
                params.update(yaml.load(yml))

        job = self.request_app('run_job', args[0], args[1], params)
        if job and 'job_name' in job:
            url = self.build_url('jobs', job['job_name'], relative=False)
            self.write(dict(job_url=url))
        else:
            message = 'could not start job for rule: %s.%s'
            self.send_error(message=message % args)


class JobHandler(RequestHandler):

    def get_job_detail(self, job):
        results = self.build_url('results.csv', relative=True, json=False)
        rule_segs = job['rule_name'].replace('.', '/')
        rule = self.build_url('rules', rule_segs, relative=False)
        rval = job.copy()
        rval['results'] = results
        rval['rule'] = rule
        return rval

    def get_job_summary(self, job):
        url = self.build_url(job['job_name'], relative=True)
        return dict(
            id=job['job_name'],
            status=job['current_stage'],
            url=url
        )

    def get(self, *args, **kwargs):
        if not len(args):
            # list all jobs and their URLs
            job_list = []
            jobs = self.request_app('get_job_histories')
            #for job in self.application.instance.history.itervalues():
            for job in jobs:
                job_list.append(self.get_job_summary(job))
            self.write(dict(jobs=job_list))
        else:
            # list specific details of job
            #job = self.application.instance.history[args[0]]
            job = self.request_app('get_job_history', args[0])
            if job:
                self.write(self.get_job_detail(job))
            else:
                # we don't have the full inferno job info for this job, but
                # maybe disco knows about it...
                jobinfo = self.get_jobinfo(args[0])
                if jobinfo:
                    self.write(dict(jobinfo=jobinfo))
                else:
                    message = "can't retrieve job info for job_id: %s"
                    self.send_error(message=message % args[0])


class JobResultHandler(RequestHandler):

    def get(self, *args, **kwargs):
        from inferno.lib.disco_ext import sorted_iterator
        from inferno.lib.result import reduce_result

        def flush_callback(stringio):
            self.write(stringio.getvalue())
            return StringIO.StringIO()

        self.set_header('Content-Type', 'text/csv; charset=UTF-8')
        job_id = args[0]
        jobinfo = self.get_jobinfo(job_id)
        if jobinfo:
            reduce_result(
                sorted_iterator(jobinfo['results']),
                output_stream=StringIO.StringIO(),
                flush_callback=flush_callback)


MAPPINGS = [
    (r"/index.json", MainHandler),
    (r"/rules.json", RuleListHandler),
    (r"/rules/([^/?]*).json", ModuleHandler),
    (r"/rules/([^/?]*)/([^/?]*).json", RuleHandler),
    (r"/jobs.json", JobHandler),
    (r"/jobs/([^/?]*).json", JobHandler),
    (r"/jobs/([^/?]*)/results.csv", JobResultHandler),
    (r"/_status/([^/?]*)", ChildUpdateHandler)
]


def launch_server(base_path, port, pipe):
    setproctitle("inferno - disco_ball")
    path = os.path.join(base_path, "disco_ball", "templates")
    application = tornado.web.Application(MAPPINGS, template_path=path)
    application.pipe = inferno.lib.daemon.unpickle_connection(pipe)
    server = HTTPServer(application)
    server.listen(port)
    IOLoop.instance().start()
