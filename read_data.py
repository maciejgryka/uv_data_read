 # -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import re
import csv
from itertools import izip


current_date = ''

# sensor reading regex
# float regex from http://stackoverflow.com/questions/385558/python-and-regex-question-extract-float-double-value

sr_regex = re.compile('''(?x)
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
                                # no line ending (avoid Windows mess)
''')

timestamp_regex = re.compile('''(?x)
    ^
        (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})    # match date like 2012-07-31
        \s                                      # whitespace
        (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})    # match time like 22:00:00
        \s                                      # whitespace
        Timestamp                               # word 'Timestamp'
                                                # no line ending (avoid Windows mess)
''')

end_data_regex = re.compile('End of data')

data_collection = {}
data_collection[1] = {}
data_collection[2] = {}
data_collection[3] = {}

current_date = ''

line = ''
header = ''

class SensorReading(object):
    def __init__(self, time='', val=0.0, sensor_type=0):
        self.time = time
        self.val = float(val)
        self.sensor_type = int(sensor_type)

    def __str__(self):
        return '{0} {1} {2}'.format(self.time, self.val, self.sensor_type)

    def __repr__(self):
        return 'SensorReading()'


class DayData(object):
    def __init__(self, date='', readings=[]):
        self.date = date
        self.readings = readings


class DataSet(object):
    def __init__(self, meta='', days=[]):
        self.meta = meta
        self.days = days


def is_timestamp(line):
    global timestamp_regex
    return timestamp_regex.match(line) is not None


def is_sensor_reading(line):
    global sr_regex
    return sr_regex.match(line) is not None


def is_data_end(line):
    global end_data_regex
    return end_data_regex.match(line) is not None


def get_date_from_timestamp(timestamp):
    global timestamp_regex
    re_obj = timestamp_regex.match(timestamp)
    if re_obj:
        return re_obj.group('date')
    else:
        return None


def get_rading_from_line(line):
    global sr_regex  
    re_obj = sr_regex.match(line)
    if not re_obj:
        return None
    else:
        return SensorReading(
            time = re_obj.group('time'),
            val = re_obj.group('val'),
            sensor_type = re_obj.group('sensor_type')
        )


def transpose_csv(file_name):
    a = izip(*csv.reader(open(file_name, 'rb')))
    csv.writer(open(file_name, 'wb')).writerows(a)


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

    # read the data
    with open(data_file, 'r') as f:
        while True:
            line = f.readline()
            if is_timestamp(line):
                # if it's a timestamp line start recording a given date, unless
                # it's already added
                current_date = get_date_from_timestamp(line)
            elif is_sensor_reading(line):
                # if it's a line with a sensor reading, add it to the dict
                reading = get_rading_from_line(line)
                if reading:
                    if current_date not in data_collection[reading.sensor_type]:
                        data_collection[reading.sensor_type][current_date] = []
                        print('reading', current_date, str(reading.sensor_type))
                    data_collection[reading.sensor_type][current_date].append(reading)
            elif not line:
                break
            else:
                header += line
    print('done reading data')

    dates = data_collection[sensor_type].keys()
    dates.sort()
    dates = dates[1:-1]

    # we'll write the CSV file transposed and transpose to the right format later
    print('writing to ',  out_file)
    times = []
    with open(out_file, 'w') as f:
        # write row of times
        # with one empty cell first
        f.write(' ,')
        for reading in data_collection[sensor_type][dates[0]]:
            times.append(reading.time)
            f.write(times[-1] + ',')

        # start new line
        f.write('\n')

        # for each date write values at given times
        for date in dates:
            f.write(date + ',')
            readings = data_collection[sensor_type][date]
            for reading in readings:
                f.write(str(reading.val) + ',')
            f.write('\n')

    print('transposing')
    transpose_csv(out_file)
