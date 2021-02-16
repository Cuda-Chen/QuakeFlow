import json
from ast import literal_eval
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import window

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
    ssc = StreamingContext(sc, 2)  # 2 second window
    broker, topic = 'localhost:2181', 'testtopic'
    kvs = KafkaUtils.createStream(ssc,
                                  broker, "spark-streaming-consumer",
                                  {topic: 1})
    # lines = kvs.map(lambda x: (json.loads(x[1])['key'], json.loads(x[1])['data']))
    lines = kvs.map(lambda x: tuple(
        (
            x[0],
            tuple(json.loads(x[1]))
        )
    ))

    ssc.checkpoint("./checkpoint-tweet")

    def dosomething(x):
        content = x.collect()
        print('########', x.collect())

    # lines.foreachRDD(dosomething)

    # df = lines.countByValueAndWindow(6, 6).map(lambda x: f"###{x}")
    # df.pprint(15)

    # [groupByKeyAndWindow]
    # windowDuration: width of the window; must be a multiple of this DStream’s batching interval.
    # slideDuration: sliding interval of the window (i.e., the interval after which the new DStream will generate RDDs);

    # data format: (station_id, (timestamp, feat_vecs))
    # -> groupby station_id
    # -> map: (station_id, [(ts_0, vec_0), (ts_1, vec_1), ...])
    grouped_df = lines.groupByKeyAndWindow(windowDuration=6, slideDuration=6)
    results = grouped_df.map(lambda x: (x[0], sorted(x[1], key=lambda y: y[0])))
    results.pprint(16)

    ssc.start()
    ssc.awaitTermination()


# Have to setup the Spark libraries first!
# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 test_spark.py > log.txt
