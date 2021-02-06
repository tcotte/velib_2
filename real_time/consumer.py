import json
from kafka import KafkaConsumer
from datetime import datetime
import time
import csv

stations = {}
consumer = KafkaConsumer("velopredict", bootstrap_servers='localhost:9092', group_id="velib-monitor-stations")
for message in consumer:
    station = json.loads(message.value.decode())
    # print(station)
    station_number = station["number"]
    contract = station["contract_name"]
    # available_bikes = station["available_bike_stands"]
    available_bikes = station["available_bikes"]
    capacity = station["bike_stands"]
    status = station["status"]
    last_update = datetime.fromtimestamp(station["last_update"] / 1000).strftime("%m/%d/%Y %H:%M")

    latitude = station["position"]["lat"]
    longitude = station["position"]["lng"]

    station_full = "Full" if available_bikes == capacity else "None"
    station_empty = "Empty" if available_bikes == 0 else "None"

    print(str(station["number"]) + " -- " + str(last_update) + " -- " + str(available_bikes))

    # data = [str(datetime.now().strftime("%m/%d/%Y %H:%M")), last_update,
    #         station_number, available_bikes, non_available, capacity, station_full, station_empty, status]
    data = [station["number"], last_update , available_bikes]

    with open('../Velib/model_analysis.csv', 'a', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(data)
