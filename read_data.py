 # -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import re
import csv
from itertools import izip
from collections import OrderedDict, defaultdict


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
        r\r?\n                 # line ending
''')

timestamp_regex = re.compile('''(?x)
    ^
        (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})    # match date like 2012-07-31
        \s                                      # whitespace
        (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})    # match time like 22:00:00
        \s                                      # whitespace
        Timestamp                               # word 'Timestamp'
        r\r?\n                                 # line ending
''')

end_data_regex = re.compile('End of data')

data_collection = {}
data_collection[1] = OrderedDict()
data_collection[2] = OrderedDict()
data_collection[3] = OrderedDict()

current_date = ''

# times of day where a reading was taken; this includes readinga from ALL dates
times = set()

line = ''
header = ''

class SensorReading(object):
    def __init__(self, time='', val=0.0, sensor_type=0):
        self.time = time
        self.val = float(val)
        self.sensor_type = int(sensor_type)

    def __str__(self):
        return str(self.val)

    def __repr__(self):
        return 'SensorReading(\'{0}\', {1}, {2})'.format(
            self.time, self.sensor_type, self.val
        )


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


def get_reading_from_line(line):
    global sr_regex  
    re_obj = sr_regex.match(line)
    if not re_obj:
        return None
    else:
        return SensorReading(
            time=re_obj.group('time'),
            val=re_obj.group('val'),
            sensor_type=re_obj.group('sensor_type')
        )


def sum_counts(data, dates, times):
    data_sum = 0.0
    # goes through each date
    for date in dates:
        # and each time
        for time in times:
            # and if the corresponding value exists, adds it to the overall
            # import pdb; pdb.set_trace()
            if str(data[date][time]) is not '-':
                # print('adding', data[date][time])
                data_sum += data[date][time]
    return data_sum

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
                # start recording a given date
                current_date = get_date_from_timestamp(line)
            elif is_sensor_reading(line):
                # sensor reading line, add it to the dict
                reading = get_reading_from_line(line)
                # accumulate only time readings for the current sensor
                if reading.sensor_type==sensor_type:
                    times.add(reading.time)
                if current_date not in data_collection[reading.sensor_type]:
                    data_collection[reading.sensor_type][current_date] = defaultdict(lambda: '-')
                    print('reading', current_date, str(reading.sensor_type))                    
                data_collection[reading.sensor_type][current_date][reading.time] = reading.val
            elif line.find('End of data') > -1:
                # the end
                break
            else:
                header += line
    print('done reading data')

    dates = data_collection[sensor_type].keys()
    dates.sort()
    # dates = dates[1:-1]

    times = list(times)
    times.sort()

    # write the CSV file
    print('writing to ',  out_file)
    with open(out_file, 'w') as f:
        # write row of times
        # with one empty cell first
        f.write(' ,')
        for time in times:
            f.write(time + ',')

        # start new line
        f.write('\n')

        # for each date write values at all times
        for date in dates:
            f.write(date + ',')
            for time in times:
                f.write(str(data_collection[sensor_type][date][time]) + ',')
            f.write('\n')


    # extract data for the report
    data_file_regex = re.compile('''(?x)
            (?P<pin>[0-9]{1,3})
            b
            (?P<b>[0-9]{1,2})
            a
            (?P<a>[0-9]{1,2})
            \.
            (?P<num>[0-9]{1})
    ''')
    dd = data_file.split('/')[-1]
    groups = data_file_regex.match(dd)
    pin = groups.groupdict()['pin']
    a = groups.groupdict()['a']
    b = groups.groupdict()['b']
    days = len(data_collection[sensor_type].keys())
    data_sum = sum_counts(data_collection[1], data_collection[1].keys(), times)

    # write the report file
    report_path = data_file + '_report.txt'
    with open(report_path, 'w') as f:
        g = groups.groupdict()
        f.write('pin\t\ta\t\tb\t\tdays\t\tsum\n')
        f.write('{0}\t\t{1}\t\t{2}\t\t{3}\t\t{4}\n'.format(pin, a, b, days, data_sum))

    print(open(report_path, 'r').read())
