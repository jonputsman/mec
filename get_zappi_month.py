#!/usr/bin/env python3
import getopt
import time
import sys
import os
from pathlib import Path
from token import EXACT_TOKEN_TYPES

import run_zappi
import mec.zp
import mec.power_meter

import csv
import pandas as pd

# This needs to have debugging disabled.

FIELD_NAMES = {'gep': 'Generation',
               'gen': 'Generated Negative',
               'h1d': 'Zappi diverted',
               'h1b': 'Zappi imported',
               'imp': 'Imported',
               'exp': 'Exported'}


class Day():

    def __init__(self, year, month, day):
        self.tm_year = year
        self.tm_mon = month
        self.tm_mday = day

    def GetString(self):
        return str(self.tm_year) + "-" + str(self.tm_mon) + "-" + str(self.tm_mday)

show_headers = True


def main():
    """Main"""
    global show_headers

    args = ['month=',
            'year=',
            'reuse_csv_files']
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', args)
    except getopt.GetoptError:
        print('Unknown options')
        print(args)
        sys.exit(2)

    reuse_csv_files = False

    today = time.localtime()
    day = Day(today.tm_year, today.tm_mon, today.tm_mday)

    for opt, value in opts:
        if opt == '--month':
            day.tm_mon = value
        elif opt == '--year':
            day.tm_year = value
        elif opt == '--reuse_csv_files':
            reuse_csv_files = True

    config = run_zappi.load_config(debug=False)

    server_conn = mec.zp.MyEnergiHost(config['username'], config['password'])
    server_conn.refresh()

    # Create output folder
    output_folder = "result-data"
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    if day.tm_mon == "all":
        if day.tm_year != today.tm_year:
            # Previous year - get all months 
            month_list = range(1, 12 + 1)
        else:
            # Current year, process months up to this one
            month_list = range(1, today.tm_mon + 1)
    else:
        # Specified month
        month_list = range(int(day.tm_mon), int(day.tm_mon) + 1)

    for month in month_list:
        day.tm_mon = month
        filename = str(day.tm_year) + "-" + str(day.tm_mon) + ".csv"
        filename = os.path.join(output_folder, filename)
        last_day_of_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        start_day = 1
        end_day = last_day_of_month[int(day.tm_mon)-1]

        GetMonthData(server_conn, day, start_day, end_day, filename, reuse_csv_files)
        ProcessMonth(filename, day.tm_year, day.tm_mon, start_day, end_day)

def GetMonthData(server_conn, day, start_day, end_day, filename, reuse_csv_files):
    headers_written = False
 
    if not os.path.exists(filename) or not reuse_csv_files: 
        f = open(filename, "w")
        csv_writer = csv.writer(f)

        # The Zappi V2.
        for zappi in server_conn.state.zappi_list():
            show_headers = True
            
            for daynum in range(start_day, end_day + 1):
                day.tm_mday = daynum

                british_summer_time = False
                if day.tm_mon >= 3 and day.tm_mon <= 10:
                    if day.tm_mon == 3:
                        if day.tm_mday >= 27:
                            british_summer_time = True
                    elif day.tm_mon == 10:
                        if day.tm_mday <= 30:
                            british_summer_time = True
                    else:
                        british_summer_time = True

                headers, data, _ = load_day(server_conn, zappi.sno, day, british_summer_time)
                if headers_written == False:
                    csv_writer.writerow(headers)
                    headers_written = True

                for d in data:
                    csv_writer.writerow(d)

        f.close()

def load_day(server_conn, zid, day, british_summer_time):

    global show_headers

    res = server_conn.get_minute_data(zid, day=day)
    prev_sample_time = -60

    #headers = ['imp', 'exp', 'gen', 'gep', 'h1d', 'h1b',
    #           'pect1', 'nect1', 'pect2', 'nect2', 'pect3', 'nect3']
    headers = ['imp', 'exp', 'gep', 'gen', 'h1d']
    table_headers = ['Date', 'Time', 'Duration']
    data = []

    for key in headers:
        if key in FIELD_NAMES:
            table_headers.append(FIELD_NAMES[key])
        else:
            table_headers.append(key)
    for rec in res:
        row = []
        hour = 0
        minute = 0
        volts = 1
        if 'imp' in rec and 'nect1' in rec and rec['imp'] == rec['nect1']:
            del rec['nect1']
        if 'exp' in rec and 'pect1' in rec and rec['exp'] == rec['pect1']:
            del rec['pect1']
        if 'hr' in rec:
            hour = rec['hr']
            del rec['hr']
        if 'min' in rec:
            minute = rec['min']
            del rec['min']

        day_copy = Day(day.tm_year, day.tm_mon, day.tm_mday)
        if british_summer_time:
            hour += 1
        if hour >= 24:
            hour = 0
            day_copy.tm_mday += 1


        sample_time = ((hour * 60) + minute) * 60

        for key in ['dow', 'yr', 'mon', 'dom']:
            del rec[key]

        if 'v1' in rec:
            volts = rec['v1'] / 10
        for key in ['v1', 'frq']:
            if key in rec:
                del rec[key]

        row.append(day_copy.GetString())
        row.append('{:02}:{:02}'.format(hour, minute))
        row.append(sample_time - prev_sample_time)

        for key in headers:
            if key in rec:
                value = rec[key]
                # Per minute data
                # Value returned should be Watt-seconds, so divide by 60 to get watts
                watts = value / 60
                row.append(int(watts))
                del rec[key]
            else:
                watts = 0
                row.append(None)
        prev_sample_time = sample_time

        data.append(row)
    num_records = len(data)
    
    print(day.GetString() + ': there are {} records'.format(num_records))

    return (table_headers, data, row)

def ProcessMonth(filename, year, month, start_day, end_day):
    dataframe = pd.read_csv(filename)
    
    # Replace nan values with 0
    dataframe = dataframe.fillna(value=0)

    output_filename = os.path.splitext(filename)[0] + "-output.csv"

    PandasProcessMonth(dataframe, year, month, start_day, end_day, output_filename)

def WattMinsToKWh(watt_seconds):
    return watt_seconds / 60 / 1000

def IsTimeOffPeak(hour_minute):
    t = hour_minute.split(":")
    minute = int(t[0]) * 60 + int(t[1])

    if (minute >= 30 and minute <= (4 * 60 + 30)):
        return True
    else:
        return False

def PandasProcessMonth(data, year, month, start_day, end_day, output_filename):
    series_list = []

    for _day in range(start_day, end_day+1):
        charged_on_peak = []
        charged_off_peak = []
        zappi_imported_on_peak = []
        zappi_imported_off_peak = []

        day_string = str(year) + "-" + str(month) + "-" + str(_day)

        day_of_interest = data['Date'].str.fullmatch(day_string) 
        my_data = pd.DataFrame(data[day_of_interest])

        for i in range(my_data.first_valid_index(), my_data.first_valid_index() + len(my_data)):
            imported = my_data['Imported'][i]
            zappi_diverted = my_data['Zappi diverted'][i]
            zappi_imported = min(imported, zappi_diverted)

            offPeak = IsTimeOffPeak(my_data['Time'][i])

            if offPeak:
                zappi_imported_off_peak.append(zappi_imported)
                charged_off_peak.append(zappi_diverted)
            else:
                zappi_imported_on_peak.append(zappi_imported)
                charged_on_peak.append(zappi_diverted)

        if sum(zappi_imported_off_peak) > 0:
            rec = {}
            rec["Date"] = day_string
            rec["Import Amount"] = WattMinsToKWh(sum(zappi_imported_off_peak))
            rec["Unit Cost"] = 7.5
            rec["Cost"] = rec["Import Amount"] * rec["Unit Cost"] / 100
            rec["kWh Charged"] = WattMinsToKWh(sum(charged_off_peak))
            series_list.append(pd.Series(rec))
        
        if sum(zappi_imported_on_peak) > 0:
            rec = {}
            rec["Date"] = day_string
            rec["Import Amount"] = WattMinsToKWh(sum(zappi_imported_on_peak))
            rec["Unit Cost"] = 30.83
            rec["Cost"] = rec["Import Amount"] * rec["Unit Cost"] / 100
            rec["kWh Charged"] = WattMinsToKWh(sum(charged_on_peak))
            series_list.append(pd.Series(rec))

    output_df = pd.concat(series_list, axis=1).transpose()

    total = {}
    total["Date"] = "Total"
    total["Import Amount"] = output_df["Import Amount"].sum()
    total["Unit Cost"] = None
    total["Cost"] = output_df["Cost"].sum()
    total["kWh Charged"] = output_df["kWh Charged"].sum()
    s = pd.Series(total)

    output_df = pd.concat([output_df, s.to_frame().transpose()], ignore_index=True)

    output_df.to_csv(output_filename)

if __name__ == '__main__':
    main()
