#!/usr/bin/env python
import pandas as pd
import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt
from json import dumps
from kafka import KafkaProducer
import pickle
import datetime
import requests
import logging
import obspy
from obspy.clients.seedlink.easyseedlink import create_client, EasySeedLinkClient
from obspy.clients.fdsn import Client
from collections import defaultdict

NETWORK = "UW"
MIN_LAT = 46.6
MAX_LAT = 48.6
MIN_LON = -123.6
MAX_LON = -121.6
CENTER = ((MIN_LON + MAX_LON)/2.0, (MIN_LAT + MAX_LAT)/2.0)
PI = 3.1415926
DEGREE2KM = PI*6371/180
CHANNELS = "HHE,HHN,HHZ"

def timestamp(x): return x.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

## realtime station information
# http://ds.iris.edu/gmap/#network=_REALTIME&starttime=2021-03-01&datacenter=IRISDMC&networktype=permanent&planet=earth
# 
# http://ds.iris.edu/gmap/#network=_REALTIME&channel=HH*&starttime=2021-03-01&datacenter=IRISDMC&networktype=permanent&planet=earth

stations = pd.read_csv("realtime-stations.txt", sep="|",  header=None, skiprows=3, names=["network", "station", "latitude", "longitude", "elevation(m)", "location", "starttime", "endtime"])
stations = stations[stations["network"] == NETWORK]

plt.figure()
plt.subplot(121)
plt.plot(stations["longitude"], stations["latitude"], '^')
plt.axis("scaled")

stations = stations[(MIN_LAT < stations["latitude"]) & (stations["latitude"] < MAX_LAT)]
stations = stations[(MIN_LON < stations["longitude"]) & (stations["longitude"] < MAX_LON)]
stations["station_id"] = stations["network"] + "." + stations["station"]
stations = stations.reset_index()
print("Selected num of stations: ", len(stations))

# plt.figure()
plt.subplot(121)
plt.plot(stations["longitude"], stations["latitude"], '^')
plt.axis("scaled")
plt.savefig("stations.png")

stations_obspy = Client("IRIS").get_stations(network = NETWORK,
                                       station = "*",
                                       starttime=obspy.UTCDateTime("2021-04-03"),
                                       minlongitude=MIN_LON,
                                       maxlongitude=MAX_LON,
                                       minlatitude=MIN_LAT,
                                       maxlatitude=MAX_LAT,
                                       channel=CHANNELS,
                                       level="response")#,
station_resp = defaultdict(dict)
for network in stations_obspy:
    for station in network:
        for chn in station:
            sid = f"{network.code}.{station.code}.{chn.location_code}.{chn.code}"            
            station_resp[sid] = chn.response.instrument_sensitivity.value



class Client(EasySeedLinkClient):
    def __init__(self, server_url, producer, autoconnect=True):
        super().__init__(server_url, producer)
        self.producer = producer
    
    def on_data(self, trace):
        print('Received trace:', trace.id)
        print(trace)
        value = {"timestamp": timestamp(trace.stats.starttime.datetime), 
                 "vec":(trace.data/station_resp[trace.id]).tolist()}
        self.producer.send('waveform_raw', key=trace.id, value=value)
            

if __name__ == '__main__':
    print('Connecting to Kafka cluster for producer...')

    # TODO Will need to clean up this with better env config
    try:
        BROKER_URL = 'quakeflow-kafka-headless:9092'
        producer = KafkaProducer(bootstrap_servers=[BROKER_URL],
                                 key_serializer=lambda x: dumps(x).encode('utf-8'),
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as error:
        print('k8s kafka not found or connection failed, fallback to local')
        BROKER_URL = 'localhost:9092'
        producer = KafkaProducer(bootstrap_servers=[BROKER_URL],
                                 key_serializer=lambda x: dumps(x).encode('utf-8'),
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))

    logging.warning('Starting producer...')

    client = Client('rtserve.iris.washington.edu:18000', producer=producer)
    for index, row in stations.iterrows():
        client.select_stream(row["network"], row["station"], "HH?")
    client.run()



