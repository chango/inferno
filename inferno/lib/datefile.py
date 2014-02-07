import os
from datetime import datetime
from datetime import timedelta


class Datefile(object):
    EPOCH = datetime(1970, 1, 1)
    DAY_OF_WEEK = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
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
        if 'oclock' in delta_spec and 'weekday' in delta_spec:
            target_dow = self.DAY_OF_WEEK.get(delta_spec['weekday'].lower())
            today = now.date()
            target = self.next_dow(today, target_dow)
            target = datetime(day=target.day, month=target.month, year=target.year, hour=delta_spec['oclock'])
            return today > self.timestamp.date() and now > target
        elif 'oclock' in delta_spec:
            today = now.date()
            target = datetime(day=today.day, month=today.month, year=today.year, hour=delta_spec['oclock'])
            return today > self.timestamp.date() and now > target
        elif 'weekday' in delta_spec:
            # Default for weekday is 4AM
            target_dow = self.DAY_OF_WEEK.get(delta_spec['weekday'].lower())
            today = now.date()
            target = self.next_dow(today, target_dow)
            target = datetime(day=target.day, month=target.month, year=target.year, hour=4)
            return today > self.timestamp.date() and now > target
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

    def next_dow(self, d, day):
        while d.weekday() != day:
            d += timedelta(1)
        return d

    def __str__(self):
        return self.timestamp.strftime(self.format)



