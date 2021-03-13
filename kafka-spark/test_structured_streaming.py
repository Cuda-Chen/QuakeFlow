#!/usr/bin/env python
# coding: utf-8

# ## Test Structured Streaming
# 
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# 
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

# ### 1. check can get waveforms from kafka

# In[1]:


from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    bootstrap_servers=['my-cluster-kafka-bootstrap:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    key_deserializer=lambda x: loads(x.decode('utf-8')),
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

consumer.subscribe(['waveform-raw', 'phasenet_picks', 'gmma_events'])


# In[2]:


for message in consumer:
    print(message)
    break


# ### 2. Structured Streaming

# In[3]:


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'


# In[4]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("spark").getOrCreate()


# In[5]:


df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap:9092").option("subscribe", "waveform-raw").load()
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")


# In[11]:


data = df.writeStream.queryName("waveform1").format("memory").start()


# In[12]:


data_ = spark.sql("select * from waveform1")
data_.show()
data_.printSchema()

# from pyspark.sql.functions import window
# import pyspark.sql.functions as F
# windowedCounts = data_.groupBy(
#     window(data_.timestamp, "10 seconds", "3 seconds"),
#     data_.key
# )
# df_new = windowedCounts.agg(F.collect_list('value'))

# df_new.awaitTermination() 

# In[13]:


rdd = data_.rdd.map(lambda x: (x["value"]))
rdd.collect()


# In[14]:


df.timestamp


# In[22]:


query = df.writeStream.outputMode("update").format("console").trigger(continuous='1 second').start()

query.awaitTermination()

