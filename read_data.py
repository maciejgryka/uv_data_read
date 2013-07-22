 # -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import re
import csv
from itertools import izip
from collections import OrderedDict, defaultdict


THRESHOLD = 50.0


timestamp_regex = re.compile('''(?x)
    ^
        (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})    # match date like 2012-07-31
        \s                                      # whitespace
        (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})    # match time like 22:00:00
        \s                                      # whitespace
        Timestamp                               # word 'Timestamp'
        r\r?\n                                 # line ending
''')


class BadgeData(object):
    """
    Represents the whole data file for a given badge. Contains <sensors>,
    a dictionary containing mapping from sensor type (int) to SensorData
    objects.

    """

    name_pattern = re.compile('''(?x)
        (?P<pin>[0-9]{1,3})
        b
        (?P<b>[0-9]{1,2})
        a
        (?P<a>[0-9]{1,2})
        \.
        (?P<num>[0-9]{1})
    ''')

    def __init__(self, data_file, header=''):
        self.header = header
        self.sensors = {}

        # extract data from the name
        self.name = data_file.split('/')[-1]
        groups = self.name_pattern.match(self.name).groupdict()
        self.pin = groups['pin']
        self.a = groups['a']
        self.b = groups['b']

    def add_reading(self, sensor_reading):
        """Add reading to the appropriate SensorData instance."""
        sensor_type = sensor_reading.sensor_type
        if sensor_type not in self.sensors.keys():
            self.sensors[sensor_type] = SensorData(sensor_type)

        self.sensors[sensor_type].add_reading(sensor_reading)


class SensorData(object):
    """Represents all sensor readings from data file."""

    def __init__(self, sensor_type):
        self.sensor_type = sensor_type

        self.readings = []
        self.values = defaultdict(lambda: None)

        self._times = set()
        # we want dates to be unique too, but also to preserve ordering, so we
        # manually keep track of what goes on there
        self.dates = []

    def add_reading(self, reading):
        """
        Add reading to the list. Also add the reading value to the dict for
        faster lookup.

        """
        self._times.add(reading.time)
        if reading.date not in self.dates:
            self.dates.append(reading.date)

        self.values['{0} {1}'.format(reading.date, reading.time)] = reading.value
        self.readings.append(reading)

    @property
    def times(self):
        return sorted(list(self._times))

    def get_value(self, date, time):
        return self.values['{0} {1}'.format(date, time)]

    def get_first_date_over(self, threshold):
        day_sums = self.get_day_sums()
        for date in self.dates:
            if day_sums[date] > threshold:
                return date
        return None

    def get_n_days(self, min_val=None):
        """
        Lazily compute and return the number of unique days in the data. If
        <min_val> is specified, only count days with sum of values at least
        <min_val>.

        """
        if min_val is None:
            if hasattr(self, 'n_days'):
                return self.n_days
            else:
                return len(set([reading.date for reading in self.readings]))
        else:
            return len(set([
                reading.date
                for reading in self.readings
                if self.get_day_sums()[reading.date] > min_val
            ]))

    def get_day_sums(self):
        if hasattr(self, 'day_sums'):
            return self.day_sums
        else:
            self.day_sums = defaultdict(lambda: 0.0)
            for reading in self.readings:
                self.day_sums[reading.date] += reading.value
            return self.day_sums

    def get_sum(self, min_val=None):
        day_sums = self.get_day_sums()
        sensor_sum = 0.0
        if min_val is None:
            return sum(day_sums[day] for day in day_sums.keys())
        else:
            return sum(
                day_sums[day]
                for day in day_sums.keys()
                if day_sums[day] > min_val
            )


class SensorReading(object):
    """Single reading from a sensor."""

    # sensor reading regex
    # float regex from http://stackoverflow.com/questions/385558/python-and-regex-question-extract-float-double-value
    pattern = re.compile('''(?x)
        ^
            (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})    # time in HH:MM:SS format
            \s                                      # whitespace
            (?P<val>
                [+-]?\ *            # first, match an optional sign *and space*
                (                   # then match integers or f.p. mantissas:
                    \d+             # start out with a ...
                    (
                        \.\d*       # mantissa of the form a.b or a.
                    )?              # ? takes care of integers of the form a
                    |\.\d+          # mantissa of the form .b
                )
                ([eE][+-]?\d+)?     # finally, optionally match an exponent
            )
            \s                      # whitespace 
            (?P<sensor_type>[1-3]*) # second value
            r\r?\n                 # line ending
    ''')

    def __init__(self, date='', time='', val=0.0, sensor_type=0):
        self.date = date
        self.time = time
        self.value = float(val)
        self.sensor_type = int(sensor_type)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return 'SensorReading(\'{0}\',\'{1}\', {2}, {3})'.format(
            self.date, self.time, self.sensor_type, self.value
        )

    @classmethod
    def from_line(cls, line, date): 
        re_obj = cls.pattern.match(line)
        if re_obj is None:
            raise RuntimeError('The following line does not match a '
                               'SensorReading pattern: {0}'.format(line))
        else:
            return SensorReading(
                date=date,
                time=re_obj.group('time'),
                val=re_obj.group('val'),
                sensor_type=re_obj.group('sensor_type')
            )

    @classmethod
    def legit(cls, line):
        """Return True if the given line represents a valid SensorReading."""
        return cls.pattern.match(line) is not None


def is_timestamp(line):
    global timestamp_regex
    return timestamp_regex.match(line) is not None


def get_date_from_timestamp(timestamp):
    global timestamp_regex
    re_obj = timestamp_regex.match(timestamp)
    if re_obj:
        return re_obj.group('date')
    else:
        return None


if __name__ == '__main__':
    n_args = len(sys.argv)
    if not (n_args == 2 or n_args == 3):
        print('Need arguments: data_file [sensor_type]')
        sys.exit()


    data_file = sys.argv[1]
    out_file = data_file + '.csv'
    sensor_type = 1
    if len(sys.argv) is 3:
        sensor_type = int(sys.argv[2])

    badge_data = BadgeData(data_file)

    current_date = ''
    # read the data
    with open(data_file, 'r') as f:
        for line in f:
            if is_timestamp(line):
                # start recording a given date
                current_date = get_date_from_timestamp(line)
            elif SensorReading.legit(line):
                # sensor reading line, add it to the dict
                reading = SensorReading.from_line(line, current_date)
                badge_data.add_reading(reading)
            else:
                badge_data.header += line
    print('done reading data')

    sensor_data = badge_data.sensors[sensor_type]

    for date in sensor_data.dates:
        print(date, sensor_data.sensor_type, sensor_data.get_day_sums()[date])


    # write the CSV file
    print('writing to ',  out_file)
    with open(out_file, 'w') as f:
        # write row of times
        # with one empty cell first
        f.write(' ,')
        for time in sensor_data.times:
            f.write(time + ',')

        # start new line
        f.write('\n')

        # for each date write values at all times
        for date in sensor_data.dates:
            f.write(date + ',')
            for time in sensor_data.times:
                value = sensor_data.get_value(date, time) or '-'
                f.write(str(value) + ',')
            f.write('\n')


    days = sensor_data.get_n_days()
    valid_days = sensor_data.get_n_days(THRESHOLD)

    # write the report file
    report_path = data_file + '_report.csv'
    with open(report_path, 'w') as f:
        f.write('pin,month a,month b,first day over {0}, days,days over {0},sum over valid days\n'.format(THRESHOLD))
        f.write('{0},{1},{2},{3},{4},{5},{6}\n'.format(
            badge_data.pin,
            badge_data.a,
            badge_data.b,
            sensor_data.get_first_date_over(THRESHOLD),
            days,
            valid_days,
            sensor_data.get_sum(THRESHOLD)
        ))

    print(open(report_path, 'r').read())
