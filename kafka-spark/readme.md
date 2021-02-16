# Kafka & Pyspark 

## Setup

1. Install Conda Env 
```
conda env create --name cs329s --file=env.yml
```

2. Run your Zookeeper and Kafka cluster

See https://kafka.apache.org/quickstart for the installation and detailed steps.

```
# Start the ZooKeeper service
$ bin/zookeeper-server-start.sh config/zookeeper.properties

# Start the Kafka broker service
$ bin/kafka-server-start.sh config/server.properties
```

3. Create a topic `testtopic` (just for test purpose)

```
$ bin/kafka-topics.sh --create --topic testtopic --bootstrap-server localhost:9092
```

4. Setup PhaseNet and GMMA

Make sure you get the submodules first

PhaseNet:

```
$ cd ./PhaseNet/phasenet
$ uvicorn app:app --reload --port 8000
```

Open another terminal and run
GMMA:

```
$ cd ./GMMA/gmma
$ uvicorn app:app --reload --port 8001
```

5. Run the `producer.py` script

You may want to comment and change the file a bit according to your needs.

6. Run the `consumer.py` script

The consumer will read the messages from the Kafka cluster.

7. Run the `test_spark.py` script for testing the Spark features

- `spark-submit` is pre-installed in our environment

- Uncomment some lines in `producer.py`, which basically sends integers to Kafka. Run it!

- Run the following command, and you will see the logs in `log.txt`

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 test_spark.py > log.txt
```
