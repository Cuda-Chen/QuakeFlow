from typing import no_type_check
import pyspark.sql.functions as F
from pyspark.sql.functions import window, from_json, col, sort_array, struct, from_utc_timestamp
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, ArrayType, FloatType
from pyspark.sql import SparkSession
import os
import numpy as np
import requests
PHASENET_API_URL = "http://localhost:8000"
BROKER_URL = 'localhost:9092'
# PHASENET_API_URL = "http://phasenet-api:8000"
# BROKER_URL = 'my-kafka-headless:9092'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

schema = StructType([StructField("timestamp", StringType()),
                     StructField("vec", ArrayType(ArrayType(FloatType())))])


spark = SparkSession.builder.appName("spark").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{BROKER_URL}").option("subscribe", "waveform_raw").load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")\
       .withColumn("value", from_json(col("value"), schema))\
       .withColumn("vec", col('value.vec'))\
       .withColumn("vec_timestamp", col('value.timestamp'))
#    .withColumn("vec_timestamp", from_utc_timestamp(col('data.timestamp'), "UTC"))\

df_window = df.withWatermark("timestamp", "31 seconds") \
    .groupBy(
    df.key,
    window("timestamp", "31 seconds", "3 seconds"),
).agg(F.collect_list('vec').alias('vec'), F.collect_list('vec_timestamp').alias('vec_timestamp'))

def foreach_batch_function(row, batch_id):
    print('>>>>>>>>>>>>>>>>')

    df_batch = row.groupby(col('key'))\
                  .agg(F.sort_array(F.collect_list(F.struct('vec_timestamp', 'vec'))).alias("collected_list"))\
                  .withColumn("vec", col("collected_list.vec"))\
                  .withColumn("vec_timestamp", col("collected_list.vec_timestamp"))

    res = df_batch.collect()
    print(f"batch_id = {batch_id}, len(res) = {len(res)}")
    if len(res) == 0:
        return None
    id_list = []
    timestamp_list = []
    NT = 3000
    vec_array = np.zeros((len(res), NT, 3))
    for i, x in enumerate(res):
        vec = x.vec[-1]
        vec = np.array(vec).reshape(-1, 3)
        vec_array[i,:vec.shape[0],:]= vec[:NT,:]
        id_list.append(x.key.strip("\"\'"))
        timestamp_list.append(x.vec_timestamp[-1][0])

    print('Phasenet & GMMA resp')
    req = {
        'id': id_list,
        'timestamp': timestamp_list,  # workaround
        "vec": vec_array.tolist(),
        "dt": 1.0 / 100
    }
    print(req["id"], req['timestamp'], vec_array.shape)
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