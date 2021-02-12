from time import sleep
from json import dumps
from kafka import KafkaProducer
import numpy as np
import pickle
from datetime import datetime
import numpy as np
import time
import requests
import matplotlib.pyplot as plt


# $ bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092


def visualize(waveforms, ids, timestamps, picks, sampling_rate):
    def normalize(x): return (x - np.mean(x)) / np.std(x)
    def calc_dt(t1, t2): return (datetime.strptime(t1, "%Y-%m-%dT%H:%M:%S.%f") - datetime.strptime(t2, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()

    plt.figure()
    for i in range(len(waveforms)):
        plt.plot(normalize(waveforms[i][:, 0]) / 6 + i, "k", linewidth=0.5)

    idx_dict = {k: i for i, k in enumerate(ids)}
    for i in range(len(picks)):
        if picks[i]["type"] == "p":
            color = "blue"
        else:
            color = "red"
        idx = int(calc_dt(picks[i]["timestamp"], timestamps[idx_dict[picks[i]["id"]]]) * sampling_rate)
        plt.plot([idx, idx], [idx_dict[picks[i]["id"]] - 0.5, idx_dict[picks[i]["id"]] + 0.5], color=color)
    plt.show()


def timestamp(x): return x.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def replay_data(producer):
    with open('fakedata.pkl', 'rb') as f:
        fakedata = pickle.load(f)

    # Load data configs
    data = fakedata['data']
    start_time = fakedata['start_time']
    sampling_rate = fakedata['sampling_rate']
    n_station = len(fakedata['station_id'])

    # Specify widow_size
    window_size = 10000

    # Replay the data according to the window_size
    idx = 0
    while idx < len(data):
        # Current timestamp
        ts = timestamp(start_time + idx / sampling_rate)

        # batch of data of window_size
        vecs = data[idx: idx + window_size].transpose([1, 0, 2])
        req = {
            'id': fakedata['station_id'],
            'timestamp': [ts] * n_station,
            "vec": vecs.tolist(),
            "dt": 1.0 / sampling_rate
        }

        # This is the part where we send req to PhaseNet and GMMA API
        # Comment out this part if you want to test Pyspark - Kafka integration
        resp = requests.get("http://localhost:8000/predict", json=req)
        visualize(vecs, fakedata['station_id'], [ts] * n_station, resp.json(), sampling_rate)
        catalog = requests.get("http://localhost:8001/predict", json={"picks": resp.json()})
        ####
        print(catalog.json())
        idx += window_size
        print(ts)

        # Send some message to Kafka
        producer.send('testtopic', value=req)
        time.sleep(3)

        if idx >= 3 * window_size:
            raise


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    replay_data(producer)

    # Uncomment this to test Kafka + Spark integration
    # for e in range(10000):
    #     print(e)
    #     x = np.zeros(e)
    #     data = e
    #     producer.send('testtopic', value=data)
    #     sleep(0.5)
