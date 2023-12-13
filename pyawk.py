#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 15:00:00 2020
Author:     ChaosHour - Kurt Larsen
Description:    This script will process a binlog file and output the results
                in a more readable format.  It will also provide a summary
                of the transactions processed.
Usage:      python3 pyawk.py --binlog_file=binlog.000001 --start_time="2020-07-08 00:00:00" --stop_time="2020-07-08 23:59:59"
Inspired by Percona's mysqlbinlog-awk.awk script found here: https://www.percona.com/blog/identifying-useful-information-mysql-row-based-binary-logs/
"""

import subprocess
import re
import argparse
from datetime import datetime, timedelta

def execute_command(binlog_file, start_time, stop_time):
    command = f'mysqlbinlog --base64-output=decode-rows -vv --start-datetime="{start_time}" --stop-datetime="{stop_time}" {binlog_file}'
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode().split('\n')

def process_output(output):
    s_type = ""
    s_count = 0
    count = 0
    insert_count = 0
    update_count = 0
    delete_count = 0
    flag = 0

    for line in output:
        if re.search(r'#15.*Table_map:.*mapped to number', line):
            print(f"Timestamp : {line.split()[0]} {line.split()[1]} Table : {line.split()[-5]}")
            flag = 1
        elif re.search(r'### INSERT INTO .*..*', line):
            count += 1
            insert_count += 1
            s_type = "INSERT"
            s_count += 1
        elif re.search(r'### UPDATE .*..*', line):
            count += 1
            update_count += 1
            s_type = "UPDATE"
            s_count += 1
        elif re.search(r'### DELETE FROM .*..*', line):
            count += 1
            delete_count += 1
            s_type = "DELETE"
            s_count += 1
        elif re.search(r'^# at ', line) and flag == 1 and s_count > 0:
            print(f"Query Type : {s_type} {s_count} row(s) affected")
            s_type = ""
            s_count = 0
        elif re.search(r'^COMMIT', line):
            print(f"[Transaction total : {count} Insert(s) : {insert_count} Update(s) : {update_count} Delete(s) : {delete_count}]")
            print("+----------------------+----------------------+----------------------+----------------------+")
            count = 0
            insert_count = 0
            update_count = 0
            delete_count = 0
            s_type = ""
            s_count = 0
            flag = 0

def main():
    parser = argparse.ArgumentParser(description='Process binlog file.')
    parser.add_argument('--binlog_file', required=True, help='The binlog file to process')
    parser.add_argument('--start_time', default=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), help='The start time for processing')
    parser.add_argument('--stop_time', default=(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'), help='The stop time for processing')

    args = parser.parse_args()

    output = execute_command(args.binlog_file, args.start_time, args.stop_time)
    process_output(output)

if __name__ == "__main__":
    main()