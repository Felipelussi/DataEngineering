#! /bin/bash

today=$(date +%Y%m%d) 
weather_report=raw_data_$today

curl "wttr.in/Casablanca" --output $weather_report

grep °C $weather_report > temperatures.txt

obs_tmp=$(head -1 temperatures.txt | tr -s " " | xargs | cut -d " " -f5)
for_tmp=$(head -3 temperatures.txt| tail -1 | tr -s " " | cut -d " " -f11)

echo "atual $obs_tmp"
echo "amanha $for_tmp"

year=$(TZ='Morocco/Casablanca' date +%Y)
month=$(TZ='Morocco/Casablanca' date +%m)
day=$(TZ='Morocco/Casablanca' date -u +%d)
hour=$(TZ='Morocco/Casablanca' date -u +%H)

echo -e "$year\t$month\t$day\t$hour\t$obs_tmp\t$for_tmp" >> rx_poc.log