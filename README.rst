uv_data_read
============

usage:
    read_data.py data_dir [threshold]

This script reads UV data in a given directory and saves out a processed report. Each data point is a sensor reading with associated date and time. There are 3 sensor types:
- 1: UVA counts
- 2: temperature (C)
- 3: battery (V)
By default only UVA counts are read. To change it open read_data.py and at the top of the file change `SENSOR_TYPE` to value other than 1.

Each file in the given directory that matches the pattern such as
   20b15a11.1
or
   149b12a9.1
is read and interpreted. For each of them a _report.txt file is generated containing information about accumulated counts for each date.

Additionally, after all files are read an overall report is created.
