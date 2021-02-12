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
    lines = kvs.map(lambda x: x[1])
    ssc.checkpoint("./checkpoint-tweet")

    def dosomething(x):
        content = x.collect()
        print('########', x.collect())

    # lines.foreachRDD(dosomething)

    df = lines.countByValueAndWindow(6, 6).transform(lambda rdd: rdd).map(lambda x: f"###{x}")
    df.pprint(15)

    ssc.start()
    ssc.awaitTermination()

# Have to setup the Spark libraries first!
# ~/spark-2.3.3-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 test_spark.py > log.txt
