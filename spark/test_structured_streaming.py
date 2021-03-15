#!/usr/bin/env python
# coding: utf-8

# ## Test Structured Streaming
#
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
#
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

# ### 1. check can get waveforms from kafka

# In[1]:


from typing import no_type_check
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import os
from json import loads
from pyspark.sql.functions import window
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import json
import requests
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numpy as np
PHASENET_API_URL = "http://localhost:8000"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

from pyspark.sql.functions import from_json, col, from_utc_timestamp
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, ArrayType, FloatType, MapType

schema = StructType(
        [
                StructField("timestamp", StringType()),
                StructField("vec", ArrayType(ArrayType(FloatType()))),
        ]
)

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
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data"))
# df = df.select(col('data.timestamp').alias('data_timestamp'))

# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")\
#         .select(col("key"), col("timestamp"), from_json(col("value"), schema).alias("data"))\
#         .select(col("key"), col("timestamp"), col('data.timestamp').alias('true_timestamp'), col('data.vec').alias('vec'))

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")\
            .select(col("key"), col("timestamp"), from_json(col("value"), schema).alias("data"))\
            .select(col("key"), col("timestamp"), col('data.timestamp').alias('vec_timestamp'), col('data.vec').alias('vec'))
            # .select(col("key"), col("timestamp"), from_utc_timestamp(col('data.timestamp'), "UTC").alias('vec_timestamp'), col('data.vec').alias('vec'))

# df = df.withColumn('val', toarray(df.value))


# df_grouped = df.withWatermark("timestamp", "2 hours") \
#     .groupBy(
#     window(df.true_timestamp, "6 seconds", "1 seconds"),
#     df.key,
# )

# df_grouped = df.withWatermark("true_timestamp", "2 hours") \
#     .groupBy(
#     window("true_timestamp", "5 seconds", "3 seconds"),
#     df.key,
# )

df_grouped = df.withWatermark("timestamp", "60 seconds") \
    .groupBy(
    df.key,
    # window("true_timestamp", "5 seconds", "3 seconds"),
    window("timestamp", "32 seconds", "3 seconds"),
    # "timestamp"
)

# dropDuplicates("guid", "eventTime")

# df_grouped = df.groupBy(
#     window("true_timestamp", "31 seconds", "3 seconds"),
#     df.key
# )


# df_grouped = df.groupBy(
#     window(df.timestamp, "31 seconds"),
#     df.key
# )

# df_grouped = df.withWatermark("timestamp", "1 minute") \
#     .groupBy(
#     window(df.timestamp, "31 seconds"),
#     df.key
# )

# df_grouped = df.withWatermark("timestamp", "1 minute") \
#     .groupBy(
#     window(df.timestamp, "31 seconds", "3 seconds"),
#     df.key,
# )

# df_agg = df_grouped.agg(F.count(F.lit(1)).alias('num_of_rows'))
# counts = df_new.count()

# df_res = df_grouped.agg(F.collect_list('val').alias('value'), F.collect_list('timestamp').alias('timestamp2'))
# df_res = df_grouped.agg(F.collect_list('val').alias('value'))
# df_res = df_grouped.agg(F.collect_list('vec').alias('value'), F.collect_list('timestamp').alias('timestamp'))
df_res = df_grouped.agg(F.collect_list('vec').alias('vec'), F.collect_list('vec_timestamp').alias('vec_timestamp'))
# df_res = df_grouped.agg(F.collect_list('vec').alias('vec'))


# df_res = df_res.groupby(
#              col('window')).agg(F.collect_list('key').alias('key'))

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


    # row.persist()
    # print(row.toJSON().collect())
    # row.printSchema()
    # row.select("key").show()
    # row.show()

    # Transformations (many aggregations)

    # df_final = row.groupby(
    #     col('window')).agg(
    #     F.collect_list('key').alias('key'),
    #     F.collect_list('value').alias('value'))

        # F.collect_list('timestamp').alias('timestamp'))

    # pandas_df = df_final.toPandas()
    # pandas_df.to_json("test.json")

    df_final = row.groupby(
        col('key')).agg(
        F.collect_list('vec').alias('vec'),
        F.collect_list('vec_timestamp').alias('vec_timestamp'))#.sort(col("vec_timestamp"))

    # df_final = row.groupby(
    #     col('key')).agg(F.collect_list('value').alias('value'), F.collect_list('timestamp').alias('timestamp')).sort(col("timestamp"))

    res = df_final.collect()
    # res = row.collect()
    # res = row.collect()
    # print(res)
    # res = row.collect()
    print(epoch_id, len(res))
    if len(res) == 0:
        return None
    id_list = []
    timestamp_list = []
    NT = 3000
    vec_array = np.zeros((len(res), NT, 3))
    for i,x in enumerate(res):
        vec = x.vec[-1]
        vec = np.array(vec).reshape(-1, 3)
        vec_array[i,:vec.shape[0],:]= vec[:NT,:]
        id_list.append(x.key.strip("\"").strip("\'"))
        timestamp_list.append(min(x.vec_timestamp[-1]))
        print(vec.shape)
    
    
    # print(df_final.toJSON().collect())
    # print(.collect())

    # df_final.printSchema()
    # df_res.show()

    # df_final = df_res.collect()
    # print(df_final)
    print('Phasenet & GMMA resp')
    req = {
        'id': id_list,
        'timestamp': timestamp_list,  # workaround
        "vec": vec_array.tolist(),
        "dt": 1.0 / 100
    }
    print(req["id"], req['timestamp'])
    try:
        resp = requests.get('{}/predict2gmma'.format(PHASENET_API_URL), json=req)
        print('Phasenet & GMMA resp', resp.json())
    except Exception as error:
        print('Phasenet & GMMA error', error)
    # return x
    # df_res.to_csv(str(int(time.time())))
    return None


query2 = df_res.writeStream \
    .format("memory")\
    .outputMode("append")\
    .foreachBatch(foreach_batch_function)\
    .start()
query2.awaitTermination()

    # .trigger(processingTime="2.9 seconds")\
# .outputMode("complete")\
# .trigger(processingTime="1 seconds") \
# .dropDuplicates("guid", "eventTime")
# .trigger(processingTime="3 seconds") \

# query = df_res.writeStream.outputMode("update").format("console").start()
# query.awaitTermination()


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
