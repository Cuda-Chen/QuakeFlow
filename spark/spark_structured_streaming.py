from typing import no_type_check
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, ArrayType, FloatType
from pyspark.sql import SparkSession
import os
import numpy as np
import requests
# PHASENET_API_URL = "http://localhost:8000"
# BROKER_URL = 'localhost:9092'
PHASENET_API_URL = "http://phasenet-api:8000"
BROKER_URL = 'quakeflow-kafka-headless:9092'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

spark = SparkSession.builder.appName("spark").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{BROKER_URL}").option("subscribe", "waveform_raw").load()

schema = StructType([StructField("timestamp", StringType()),
                     StructField("vec", ArrayType(ArrayType(FloatType())))])
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")\
    .withColumn("key", F.regexp_replace('key', '"', ''))\
    .withColumn("value", F.from_json(col("value"), schema))\
    .withColumn("vec", col('value.vec'))\
    .withColumn("vec_timestamp", col('value.timestamp'))\
    .withColumn("vec_timestamp_utc", F.from_utc_timestamp(col('value.timestamp'), "UTC"))

df_window = df.withWatermark("vec_timestamp_utc", "1.5 seconds") \
    .groupBy(df.key, F.window("vec_timestamp_utc", "30 seconds", "3 seconds"))\
    .agg(F.sort_array(F.collect_list(F.struct('vec_timestamp_utc', 'vec_timestamp', 'vec'))).alias("collected_list"))\
    .filter(F.size(col("collected_list")) == 30)\
    .withColumn("vec", F.flatten(col("collected_list.vec")))\
    .withColumn("vec_timestamp", col("collected_list.vec_timestamp").getItem(0))\
    .drop("collected_list")


def foreach_batch_function(df_batch, batch_id):
    print(f'>>>>>>>>>>>>>>>> {batch_id} >>>>>>>>>>>>>>>>')

    df_batch = df_batch.groupby(col('window'))\
        .agg(F.collect_list('key').alias('key'), F.collect_list('vec_timestamp').alias('timestamp'), F.collect_list('vec').alias('vec'))\
        .sort(col("window"))

    res = df_batch.collect()
    for x in res:
        req = {
            'id': x.key,
            'timestamp': x.timestamp,  # workaround
            "vec": x.vec,
            "dt": 1.0 / 100
        }

        try:
            resp = requests.get('{}/predict2gmma'.format(PHASENET_API_URL), json=req)
            print('Phasenet & GMMA resp', resp.json())
        except Exception as error:
            print('Phasenet & GMMA error', error)

    return None


query = df_window.writeStream \
    .format("memory")\
    .outputMode("append")\
    .foreachBatch(foreach_batch_function)\
    .start()
query.awaitTermination()
