#!/usr/bin/env python
# coding: utf-8

# ## Test Structured Streaming
#
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
#
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

# ### 1. check can get waveforms from kafka

# In[1]:


from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import os
# from kafka import KafkaConsumer
from json import loads
from pyspark.sql.functions import window
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import json
import requests
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import *
PHASENET_API_URL = "http://localhost:8000"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'


# Use udf to define a row-at-a-time udf
@udf(IntegerType())
def foo(x):
    print('12345')
    return x


@udf(ArrayType(ArrayType(FloatType())))
def toarray(x):
    return json.loads(x)


spark = SparkSession.builder.appName("spark").getOrCreate()


df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "waveform_raw").load()
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")


df = df.withColumn('val', toarray(df.value))


df_grouped = df.withWatermark("timestamp", "1 minute") \
    .groupBy(
    window(df.timestamp, "31 seconds", "3 seconds"),
    df.key,
    "timestamp"
)

df_agg = df_grouped.agg(F.count(F.lit(1)).alias('num_of_rows'))
# counts = df_new.count()

df_res = df_grouped.agg(F.collect_list('val').alias('value'), F.collect_list('timestamp').alias('timestamp2'))

# df_final = df_res.groupby(
#     col('window')).agg(
#         F.collect_list('key').alias('key'),
#         F.collect_list('value').alias('value'),
#     F.collect_list('timestamp').alias('timestamp'))
# df_final = df_final.collect()

# df_new = data.withColumn('val', toarray(data.value))


# df_new = counts.withColumn('val', toarray(F.col('count')))

# df_new.collect()

# query = df.writeStream.queryName("waveform1").format("memory").start()

# query2 = df_new.writeStream.outputMode("update").format("console").trigger(continuous='3 second').start()


def foreach_batch_function(row, epoch_id):
    # row.cache()
    print('>>>>>>>>>>>>>>>>')

    row.show()
    # Transformations (many aggregations)
    # df_final = df_res.groupby(
    #     col('window')).agg(
    #     F.collect_list('key').alias('key'),
    # F.collect_list('value').alias('value'),
    #     F.collect_list('timestamp').alias('timestamp'))

    # df_final.printSchema()
    # df_res.show()
    # df_final = df_res.collect()
    # print(df_final)
    print('Phasenet & GMMA resp')
    req = {
        'id': None,
        'timestamp': None,  # workaround
        "vec": None,
        "dt": 1.0 / 100
    }
    try:
        resp = requests.get('{}/predict2gmma'.format(PHASENET_API_URL), json=req)
        print('Phasenet & GMMA resp', resp.json())
    except Exception as error:
        print('Phasenet & GMMA error', error)
    # return x
    # df_res.to_csv(str(int(time.time())))
    return None


query = df_res.writeStream.outputMode("update").format("console").start()
query2 = df_res.writeStream \
    .format("memory")\
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()
query2.awaitTermination()


# data_ = spark.sql("select * from waveform1")
# data_.show()
# data_.printSchema()

# df_grouped = data_.groupBy(
#     window(data_.timestamp, "10 seconds", "3 seconds"),
#     data_.key,
# )

# df_new = df_grouped.agg(F.collect_list('value'))

# df_new.show()


# rdd = data_.rdd.map(lambda x: (x["value"]))
# rdd.collect()


# def myfunc(x):
#     print('456')
#     print('Phasenet & GMMA resp')
#     req = {
#         'id': None,
#         'timestamp': None,  # workaround
#         "vec": None,
#         "dt": 1.0 / 100
#     }
#     try:
#         resp = requests.get('{}/predict2gmma'.format(PHASENET_API_URL), json=req)
#         print('Phasenet & GMMA resp', resp.json())
#     except Exception as error:
#         print('Phasenet & GMMA error', error)
#     return x


# t = df_new.rdd.map(myfunc)
# t.collect()
