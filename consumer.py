import json
from kafka import KafkaConsumer
from datetime import datetime
import time
import csv

stations = {}
consumer = KafkaConsumer("velib", bootstrap_servers='localhost:9092', group_id="velib-monitor-stations")
for message in consumer:
    station = json.loads(message.value.decode())
    # print(station)
    station_number = station["number"]
    contract = station["contract_name"]
    available_bikes = station["available_bike_stands"]
    non_available = station["available_bikes"]
    capacity = station["bike_stands"]
    status = station["status"]
    last_update = station["last_update"]

    latitude = station["position"]["lat"]
    longitude = station["position"]["lng"]

    station_full = "Full" if available_bikes == capacity else "None"
    station_empty = "Empty" if available_bikes == 0 else "None"

    print(str(datetime.now().strftime("%m/%d/%Y %H:%M")), " -- ", last_update, " -- ",
          station_number, " -- ", available_bikes, " -- ",
          non_available, " -- ", capacity, " -- ", station_full, " -- ", station_empty, " -- ", status, " -- ", latitude,
          " -- ", longitude)

    data = [str(datetime.now().strftime("%m/%d/%Y %H:%M")), last_update,
            station_number, available_bikes, non_available, capacity, station_full, station_empty, status]

    with open('velib.csv', 'a', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(data)