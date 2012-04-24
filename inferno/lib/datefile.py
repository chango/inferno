import os
from datetime import datetime
from datetime import timedelta


class Datefile(object):
    EPOCH = datetime(1970, 1, 1)
    def __init__(self, pid_dir, file_name, format='%Y-%m-%d %H:%M:%S', timestamp=None):
        if not os.path.exists(pid_dir):
            os.mkdir(pid_dir)
        self.format = format
        self.path = os.path.join(pid_dir, file_name)
        if os.path.exists(self.path) and timestamp is None:
            with open(self.path) as f:
                self.timestamp = datetime.strptime(f.read().strip(), format)
        elif timestamp is None:
            # ensure newly created Datefiles are 'old' by default
            self.touch(self.EPOCH)
        else:
            self.touch(timestamp)

    def is_older_than(self, delta_spec=None):
        now = datetime.utcnow()
        return self.timedelta(delta_spec) < now

    def timedelta(self, delta_spec=None):
        if not delta_spec:
            delta_spec = {'hours':1}
        delta = timedelta(**delta_spec)
        return self.timestamp + delta

    def touch(self, new_timestamp=None):
        if new_timestamp is None:
            new_timestamp = datetime.utcnow()
        with open(self.path, 'w') as f:
            f.write(new_timestamp.strftime(self.format))
        self.timestamp = new_timestamp

    def __str__(self):
        return self.timestamp.strftime(self.format)



