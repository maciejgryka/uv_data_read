 # -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import os
import sys
import re
import csv
import time
from datetime import datetime, timedelta
from itertools import izip
from collections import OrderedDict, defaultdict


THRESHOLD = 50.0
MINUTE_THRESHOLD = 1023
SENSOR_TYPE = 1


timestamp_pattern = re.compile('''(?x)
    ^
        (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})    # match date like 2012-07-31
        \s                                      # whitespace
        (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})    # match time like 22:00:00
        \s                                      # whitespace
        Timestamp                               # word 'Timestamp'
        r\r?\n                                 # line ending
''')


def daterange(start_date, n_days, format='%Y-%m-%d'):
    """
    Generator that returns strings for n_days after start_date (excluding the
    last date).

    """
    start_date = datetime.fromtimestamp(time.mktime(time.strptime(start_date, format)))
    end_date = start_date + timedelta(n_days)
    for d in range(int((end_date - start_date).days)):
        yield str((start_date + timedelta(d)).date())


class BadgeData(object):
    """
    Represents the whole data file for a given badge. Contains <sensors>,
    a dictionary containing mapping from sensor type (int) to SensorData
    objects.

    """

    name_pattern = re.compile('''(?x)
        ^
            (?P<pin>[0-9]{1,3})
            b
            (?P<b>[0-9]{1,2})
            a
            (?P<a>[0-9]{1,2})
            \.
            (?P<num>[0-9]{1})
        $
    ''')

    badge_id_pattern = re.compile('Badge ID:\s(?P<badge_id>[0-9]{4})')

    _badge_id = '-'

    def __init__(self, data_file, header=''):
        self.header = header
        self.sensors = {}

        # extract data from the name
        self.name = self.get_name_from_path(data_file)
        groups = self.name_pattern.match(self.name).groupdict()
        self.pin = groups['pin']
        self.a = groups['a']
        self.b = groups['b']

        self.parse(data_file)

    @classmethod
    def get_name_from_path(cls, file_path):
        """Get data file name from <file_path>, or None if invalid."""
        _, name = os.path.split(file_path)
        if cls.name_pattern.match(name) is None:
            return None
        return name

    @property
    def badge_id(self):
        return self._badge_id

    @badge_id.setter
    def badge_id(self, value):
        self._badge_id = value

    def add_to_header(self, line):
        """Add line to header and do some processing on it."""
        match = self.badge_id_pattern.match(line)
        if match:
            self.badge_id = match.group('badge_id')
        self.header += line

    def parse(self, data_file):
        current_date = ''
        # read the data
        with open(data_file, 'r') as f:
            for line in f:
                # try to interpret the line both as timestampt and as a
                # sensor reading; the wrong one will be None
                timestamp = get_date_from_timestamp(line)
                sensor_reading = SensorReading.from_line(line, current_date)
                if timestamp:
                    # start recording a given date
                    current_date = get_date_from_timestamp(line)
                elif sensor_reading:
                    # sensor reading line, add it to the dict
                    self.add_reading(sensor_reading)
                else:
                    # if it's neither a timestamp nor a sensor reading it is
                    # a part of the header
                    self.add_to_header(line)

    def add_reading(self, sensor_reading):
        """Add reading to the appropriate SensorData instance."""
        sensor_type = sensor_reading.sensor_type
        if sensor_type not in self.sensors.keys():
            self.sensors[sensor_type] = SensorData(sensor_type)

        self.sensors[sensor_type].add_reading(sensor_reading)


class SensorData(object):
    """Represents all sensor readings from a data file."""

    def __init__(self, sensor_type):
        self.sensor_type = sensor_type

        self.readings = []
        self.values = defaultdict(lambda: None)

        self._times = set()
        # we want dates to be unique too, but also to preserve ordering, so we
        # will manually keep track of what goes in there
        self._dates = []

    def add_reading(self, reading):
        """
        Add <reading> to the list. Also add the reading value to the <values>
        dict for faster lookup later.

        """
        self._times.add(reading.time)
        if reading.date not in self._dates:
            self._dates.append(reading.date)

        self.values[reading.date + reading.time] = reading.value
        self.readings.append(reading)

    @property
    def times(self):
        return sorted(list(self._times))

    @property
    def dates(self):
        """
        Disregard the first day, it's usually dodgy and anyway not important
        (it's the day the sensors are tunerd on, packed and sent to patients.)
        
        """
        return self._dates[1:]

    def get_value(self, date, time):
        return self.values[date + time]

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

    def get_sum_over_n_days(self, start_date, n_days):
        day_sums = self.get_day_sums()
        s = 0.0
        for date in daterange(start_date, n_days):
            s += day_sums[date]
        return s

    def get_avg_daily_minutes_over(self, minute_threshold, day_theshold=0.0):
        """
        Return mean daily number of minutes over the specified threshold. If
        <day_threshold> is specified, only days with sums larger or equal to it
        are included.

        """
        day_sums = self.get_day_sums()
        days = [day[0] for day in day_sums.items() if day[0] >= day_theshold]

        daily_counts = []
        for day in days:
            n_readings = sum(
                1
                for reading in self.readings 
                if reading.date == day and reading.value >= minute_threshold
            )
            daily_counts.append(n_readings)
        return float(sum(daily_counts)) / len(daily_counts)

    def write_to_csv(self, file_path):
        with open(file_path, 'w') as f:
            # write row of times
            # with one empty cell first
            f.write(' ,')
            for time in self.times:
                f.write(time + ',')

            # start new line
            f.write('\n')

            # for each date write values at all times
            for date in self.dates:
                f.write(date + ',')
                for time in self.times:
                    value = self.get_value(date, time) or '-'
                    f.write(str(value) + ',')
                f.write('\n')


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
        if re_obj:
            # sometimes data files are corrupted...
            try:
                return SensorReading(
                    date=date,
                    time=re_obj.group('time'),
                    val=re_obj.group('val'),
                    sensor_type=re_obj.group('sensor_type')
                )
            except:
                print('\n\n\tWARNING: ignoring invalid line at date {0}: '
                      '\"{1}\"\n'.format(date, line.strip('\n')))
                return None
        else:
            return None

    @classmethod
    def legit(cls, line):
        """Return True if the given line represents a valid SensorReading."""
        return cls.pattern.match(line) is not None


def get_date_from_timestamp(timestamp):
    groups = timestamp_pattern.match(timestamp)
    if groups:
        return groups.group('date')
    else:
        return None


def main():
    n_args = len(sys.argv)
    if not (n_args == 2 or n_args == 3):
        print('Need arguments: data_dir [threshold]')
        sys.exit()

    sensor_type = SENSOR_TYPE

    threshold = THRESHOLD
    if len(sys.argv) is 3:
        threshold = float(sys.argv[2])

    minute_threshold = MINUTE_THRESHOLD

    data_dir = sys.argv[1]


    # write CSV header
    report_path = os.path.join(data_dir, 'report.csv')
    with open(report_path, 'w') as f:
        f.write(
            'pin,badge_id,month b,month a,first date over {threshold},'
            'first date over {threshold} (month),'
            '7 day sum from the first day over {threshold},'
            'number of days overall,sum over all days,'
            'number days over {threshold},sum for days over {threshold},'
            'average count for days over {threshold},'
            'avg. minutes over {minute_threshold}\n'.format(
                threshold=int(threshold),
                minute_threshold=int(minute_threshold)
            )
        )

    for data_file in os.listdir(data_dir):
        data_file = os.path.abspath(os.path.join(data_dir, data_file))
        data_file_name = BadgeData.get_name_from_path(data_file)
        if data_file_name is None:
            continue

        print('reading ', data_file, '... ', end='')
        out_file = data_file + '.csv'

        badge_data = BadgeData(data_file)

        sensor_data = badge_data.sensors[sensor_type]

        with open(data_file + '_report.txt', 'w') as f:
            f.write('Sensor {0} data for {1} (badge id: {2})\n\n'.format(
                sensor_type, badge_data.name, badge_data.badge_id
            ))
            for date in sensor_data.dates:
                f.write('{0}\t{1}\t{2}\n'.format(date, sensor_data.sensor_type,
                    sensor_data.get_day_sums()[date]
                ))

        # write the CSV data file
        sensor_data.write_to_csv(out_file)
        print('done')

        # write the report file
        with open(report_path, 'a') as f:
            sensor_data = badge_data.sensors[sensor_type]
            first_valid_day = sensor_data.get_first_date_over(threshold) or '-'
            first_valid_month = '-'
            sum_seven_days = '-'
            if first_valid_day != '-':
                try:
                    first_valid_month = time.strptime(first_valid_day, '%Y-%m-%d').tm_mon
                    sum_seven_days = sensor_data.get_sum_over_n_days(first_valid_day, 7)
                except:
                    print('\n\tWARNING: invalid date: \"{0}\"\n'.format(first_valid_day))
                    first_valid_month = '-'
            n_days = sensor_data.get_n_days()
            sum_all_days = sensor_data.get_sum()
            n_valid_days = sensor_data.get_n_days(threshold)
            sum_valid_days = sensor_data.get_sum(threshold)
            avg_valid_days = sum_valid_days / n_valid_days if n_valid_days else '-'
            avg_daily_minutes_over = sensor_data.get_avg_daily_minutes_over(minute_threshold, threshold)
            
            f.write('{pin},{badge_id},{b},{a},{first_valid_day},{first_valid_month},'
                    '{sum_seven_days},{n_days},{sum_all_days},{n_valid_days},{sum_valid_days},'
                    '{avg_valid_day},{avg_daily_minutes_over}\n'.format(
                        pin=badge_data.pin,
                        badge_id=badge_data.badge_id,
                        b=badge_data.b,
                        a=badge_data.a,
                        first_valid_day=first_valid_day,
                        first_valid_month=first_valid_month,
                        sum_seven_days=sum_seven_days,
                        n_days=n_days,
                        sum_all_days=sum_all_days,
                        n_valid_days=n_valid_days,
                        sum_valid_days=sum_valid_days,
                        avg_valid_day=avg_valid_days,
                        avg_daily_minutes_over=avg_daily_minutes_over
            ))
    # print(open(report_path, 'r').read())


if __name__ == '__main__':
    main()
