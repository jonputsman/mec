import argparse
import pandas as pd
import datetime as dt

def load_zappi_data(filename):
    zappi_df = pd.read_csv(filename)
    # Replace nan values with 0
    zappi_df = zappi_df.fillna(value=0)
    
    # Convert Time and Date fields into datetime
    zappi_df['Date'] = pd.to_datetime(zappi_df['Date'], format='%Y-%m-%d').dt.date
    zappi_df['Time'] = pd.to_datetime(zappi_df['Time'], format='%H:%M').dt.time

    def WattMinsToKWh(watt_seconds):
        return watt_seconds / 60 / 1000

    # Zappi import is the smaller of Imported and Zappi Diverted
    zappi_df['Zappi Imported'] = zappi_df[['Imported', 'Zappi diverted']].min(axis=1)
    zappi_df['Zappi Imported kWh'] = zappi_df['Zappi Imported'].apply(WattMinsToKWh)

    return zappi_df


def load_tariff_date(filename):
    tariff_df = pd.read_csv(filename)
    tariff_df['Date'] = pd.to_datetime(tariff_df['Date'], format='%Y-%m-%d').dt.date
    tariff_df['Time'] = pd.to_datetime(tariff_df['Time'], format='%H:%M:%S').dt.time

    return tariff_df


def calculate_charging_cost(zappi_input_df, tariff_df):
    zappi_df = zappi_input_df.copy()

    is_zappi_charging = zappi_df['Zappi diverted'] > 0
    zappi_charging_df = zappi_df[is_zappi_charging].copy()

    charging_cost = []
    last_tariff_idx = 0
    old_date = None

    for _, row in zappi_charging_df.iterrows():
        zappi_date = row['Date']
        zappi_time = row['Time']

        cost = 0
        cost_found = False

        if old_date != zappi_date:
            print(zappi_date)
            old_date = zappi_date
        
        for i in range(last_tariff_idx, len(tariff_df)-1):
            tariff_date = tariff_df.iloc[i]['Date']
            tariff_time = tariff_df.iloc[i]['Time']
            tariff_date_next = tariff_df.iloc[i+1]['Date']
            tariff_time_next = tariff_df.iloc[i+1]['Time']

            tariff_datetime = dt.datetime.combine(tariff_date, tariff_time)
            tariff_datetime_next = dt.datetime.combine(tariff_date_next, tariff_time_next)
            zappi_datetime = dt.datetime.combine(zappi_date, zappi_time)

            if zappi_datetime >= tariff_datetime and zappi_datetime <= tariff_datetime_next:
                cost_found = True
                cost = tariff_df.iloc[i]['value_inc_vat']
                charging_cost.append(cost)
                last_tariff_idx = i
                break

        if cost_found == False:
            print('Cost not found - assuming 15p')
            charging_cost.append(15)

    zappi_charging_df['Charging unit cost'] = charging_cost
    zappi_charging_df['Charge cost'] = zappi_charging_df['Zappi Imported kWh'] * zappi_charging_df['Charging unit cost']
    
    return zappi_charging_df


def calculate_charging_summary(charging_df):
    columns = ['Date', 'Zappi diverted', 'Zappi Imported',
       'Zappi Imported kWh', 'Charge cost']

    temp_df = charging_df.copy()
    charge_summary_df = temp_df[columns].groupby('Date').sum()
    charge_summary_df['Charge cost'] = charge_summary_df['Charge cost'] / 100
    charge_summary_df['Charging unit cost'] = charge_summary_df['Charge cost'] / charge_summary_df['Zappi Imported kWh']
    
    print(charge_summary_df)
    print("Total charging cost: %f" % charge_summary_df['Charge cost'].sum())

    return charge_summary_df


def command_line():
    parser = argparse.ArgumentParser()

    parser.add_argument('-tariff', required=True)
    parser.add_argument('-energy_consumed', required=True)
    parser.add_argument('-output', required=True)
    parser.add_argument('-output_summary', required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = command_line()

    zappi_df = load_zappi_data(args.energy_consumed)
    tariff_df = load_tariff_date(args.tariff)

    zappi_charging_df = calculate_charging_cost(zappi_df, tariff_df)
    zappi_charging_df.to_csv(args.output)

    charging_summary_df = calculate_charging_summary(zappi_charging_df)
    charging_summary_df.to_csv(args.output_summary)




