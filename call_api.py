import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "3e5fa38e4550ea5cb5435b97506b7c1a0bdd370d"  # FIXME Set your own API key here
url = "https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        #producer.send("velib", json.dumps(station).encode())
        producer.send("velib", key=json.dumps(station["number"]).encode(), value=json.dumps(station).encode())
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(10)
