#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import threading
import obspy
from obspy.clients.seedlink.easyseedlink import create_client, EasySeedLinkClient


# ## realtime station information
# http://ds.iris.edu/gmap/#network=_REALTIME&starttime=2021-03-01&datacenter=IRISDMC&networktype=permanent&planet=earth
# 
# http://ds.iris.edu/gmap/#network=_REALTIME&channel=HH*&starttime=2021-03-01&datacenter=IRISDMC&networktype=permanent&planet=earth

# In[2]:


stations = pd.read_csv("realtime-stations.txt", sep="|",  header=None, skiprows=3, names=["network", "station", "latitude", "longitude", "elevation(m)", "location", "starttime", "endtime"])
stations = stations[stations["network"] == "UW"]


# In[3]:


plt.figure()
plt.plot(stations["longitude"], stations["latitude"], '^')
plt.axis("scaled")


# In[8]:


stations = stations[(46.5 < stations["latitude"]) & (stations["latitude"] < 49)]
stations = stations[(-125 < stations["longitude"]) & (stations["longitude"] < -123.5)]
stations["station_id"] = stations["network"] + "." + stations["station"]
stations = stations.reset_index()
print(len(stations))
plt.figure()
plt.plot(stations["longitude"], stations["latitude"], '^')
plt.axis("scaled")


# In[10]:


stations


# In[12]:


class Client(EasySeedLinkClient):
    def on_data(self, trace):
        print('Received trace:', trace.id)
        print(trace)
            
client = Client('rtserve.iris.washington.edu:18000')
for index, row in stations.iterrows():
    client.select_stream(row["network"], row["station"], "HH?")
client.run()


# In[ ]:




