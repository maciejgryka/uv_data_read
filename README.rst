uv_data_read
============
This code was written to make lives of some nutrition researchers less painful :) It reads out data from text files dumped by small, wearable UV sensors and creates summaries in the form of CSV reports.

Feel free to use it for your own purposes! If you do I would really like it if you let me know how it was useful to you.


    usage: read_data.py data_dir [threshold=50.0]


This script reads UV data in a given directory and saves out a processed report. Each data point is a sensor reading with associated date and time. There are 3 sensor types:

1. UVA counts
2. temperature (C)
3. battery (V)

By default only UVA counts are read. To change it open ``read_data.py`` and at the top of the file change ``SENSOR_TYPE`` to value other than ``1``.

If ``threshold`` is given only days with sum of counts over the ``threshold`` are counted. If not specified, the threshold is set to 50.0 by default.

Each file in the given directory that matches the pattern such as ``20b15a11.1`` or ``149b12a9.1`` is read and interpreted. For each of them a ``_report.txt`` file is generated containing information about accumulated counts for each date.

Additionally, after all files are read overall ``report.csv`` is created in the same directory as the files.
