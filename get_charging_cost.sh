#!/bin/bash

year=2023
month=9

zappi_out=results/$year-$month.csv
tariff_out=results/$year-$month-tariff_cost.csv
charging_cost_out=results/$year-$month-charging_cost.csv
charging_summary_out=results/$year-$month-charging_summary.csv

python get_zappi_month.py --month $month

cd ../octopus
python get_tariff.py -credentials secrets.json -year $year -month $month -output $tariff_out

cd ../zappi-github
python calc_charging_cost.py -tariff ../octopus/$tariff_out -energy_consumed $zappi_out -output $charging_cost_out -output_summary $charging_summary_out

