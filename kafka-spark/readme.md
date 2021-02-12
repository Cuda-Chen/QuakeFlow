# Kafka & Pyspark 

## Setup

1. Install Conda Env (the env file is located at the root directory)

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

4. Run the `producer.py` script

You may want to comment and change the file a bit according to your needs.

5. Run the `consumer.py` script

The consumer will read the messages from the Kafka cluster.

6. Run the `test_spark.py` script for testing the Spark features

- Install Spark and setup the env for Spark https://spark.apache.org/downloads.html. I was using `spark-2.3.3-bin-hadoop2.7`, but I think you should choose `2.4.7` and it should work too 

- Uncomment some lines in `producer.py`, which basically sends integers to Kafka

- Run the following command, and you will see the logs in `log.txt`

```
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 test_spark.py > log.txt
```

*Note: This will depends on the version of Spark you installed... If you installed the `2.4.7` ver, might need to change some version numbers in the command









