import streamlit as st
from collections import defaultdict
from kafka import KafkaConsumer
from json import loads
import time
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import PIL
from PIL import Image
import streamlit.components.v1 as components
import os
import tweepy
import logging
import sys
from collections import deque
from geopy.geocoders import Nominatim


# Streamlit layout CSS
st.markdown(
    f"""
<style>
    .reportview-container .main .block-container{{
        max-width: 100vw;
        padding-top: 1rem;
        padding-right: 1rem;
        padding-left: 1rem;
        padding-bottom: 1rem;
    }}
    .reportview-container .main {{
        color: black;
        background-color: white;
    }}
</style>
""",
    unsafe_allow_html=True,
)

# Lambdas and Constants


def normalize(x): return (x - np.mean(x) + np.finfo(x.dtype).eps) / (np.std(x) + np.finfo(x.dtype).eps)


def timestamp_seconds(x): return datetime.fromisoformat(x).timestamp()


wave_dict = defaultdict(list)
pick_dict = defaultdict(list)
event_dict = defaultdict(dict)
event_min_gap = 5
window_length = 100
window_number = 60
hop_length = 10
num_sta = 16
refresh_sec = 1.0
dt = 0.01
map_width = 900
map_height = 650
map_zoom = 9
prev_event_bundle = None
prev_event_bundle = (0.0, 0.0, 0.0, 0.0)
BOT_MAGNITUDE_THRESHOLD = 1.5
GEOLOC_TOUT = 5 # in seconds

consumer = None
# Connection to Kafka
try:
    print('Connecting to k8s kafka')
    BROKER_URL = 'quakeflow-kafka:9092'
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER_URL],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        key_deserializer=lambda x: loads(x.decode('utf-8')),
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    print('k8s kafka connection success!')
    consumer.subscribe(['waveform_raw', 'waveform_raw2', 'phasenet_picks', 'gmma_events'])
except BaseException:
    print('k8s Kafka connection error')

    try:
        print('Connecting to local kafka')
        BROKER_URL = 'localhost:9092'
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER_URL],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            key_deserializer=lambda x: loads(x.decode('utf-8')),
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        print('local kafka connection success!')
        consumer.subscribe(['waveform_raw', 'waveform_raw2', 'phasenet_picks', 'gmma_events'])

    except BaseException:
        print('local Kafka connection error')

if not consumer:
    print('No kafka server found!')


# consumer.subscribe(['waveform_raw', 'phasenet_picks'])
# consumer.subscribe(['waveform_raw'])
# consumer.subscribe(['phasenet_picks'])

# Setting up Tweepy
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
print(consumer_key)
print(consumer_secret)
print(access_token)
print(access_token_secret)

logger = logging.getLogger()


def create_api():
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    access_token = os.getenv("ACCESS_TOKEN")
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True)
        api.verify_credentials()
        logger.info("API created")
        return api
    except Exception as e:
        logger.error("Error creating API", exc_info=True)
    return None


api = create_api()

# Functions

def latlon2address(lat, lon, geolocator):
    try:
        location = geolocator.reverse(f"{lat}, {lon}")
        print(location)
        return location.address
    except:
        return None

geolocator = Nominatim(user_agent="https", timeout=5)


def update_figure_layout(figure):
    figure.update_layout(
        mapbox_style="white-bg",
        mapbox_layers=[
            {
                "below": 'traces',
                "sourcetype": "raster",
                "sourceattribution": "United States Geological Survey",
                "source": [
                    "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
                ]
            }
        ])
    figure.update_layout(
        showlegend=True,
        width=map_width,
        height=map_height,
        geo=dict(
            landcolor='rgb(217, 217, 217)',
            lonaxis=dict(
                showgrid=True,
                gridwidth=0.05,
                range=[-116.0304497751, -115.0304497751],
                dtick=5
            ),
            lataxis=dict(
                showgrid=True,
                gridwidth=0.05,
                range=[32.4800184066, 33.4800184066],
                dtick=5
            )
        ),
    )
    figure.update_layout(margin={"r": 0.5, "t": 0.5, "l": 0, "b": 0})
    return figure


def get_plot_picks(message, t0, tn):
    t0_idx = 0
    t_picks = []
    colors = []
    for i, x in enumerate(message):
        if timestamp_seconds(x["timestamp"]) >= t0:
            if t0_idx == 0:
                t0_idx = i
            if timestamp_seconds(x["timestamp"]) <= tn:
                t_picks.append(timestamp_seconds(x["timestamp"]) - t0)
                if x["type"] == "p":
                    colors.append("b")
                elif x["type"] == "s":
                    colors.append("r")
                else:
                    raise("Phase type error!")
            else:
                return t_picks, colors, t0_idx
    return t_picks, colors, t0_idx


def get_plot_events(message, t0, tn):
    t0_idx = 0
    t_events = []
    mag_events = []
    loc_events = []
    for k, x in message.items():
        if timestamp_seconds(x["time"]) >= t0:
            # if t0_idx == 0:
            #     t0_idx = i
            if timestamp_seconds(x["time"]) <= tn - 8:
                t_events.append(timestamp_seconds(x["time"]) - t0)
                mag_events.append(x["magnitude"])
                loc_events.append(x["location"])
            else:
                return t_events, mag_events, loc_events, t0_idx
    return t_events, mag_events, loc_events, t0_idx


def lng_from_x(x):
    lng = (1 / 111.1666666667) * x - 116.0304497751
    return lng


def lat_from_y(y):
    lat = (1 / 111.1537242472) * y + 32.4800184066
    return lat


def xy_list_to_latlng_list(x_list, y_list):
    lng_list = [lng_from_x(x) for x in x_list]
    lat_list = [lat_from_y(y) for y in y_list]
    return lng_list, lat_list


def loc_events_organize(loc_events):
    x_list = [event[0] for event in loc_events]
    y_list = [event[1] for event in loc_events]
    z_list = [event[2] for event in loc_events]
    lng_list, lat_list = xy_list_to_latlng_list(x_list, y_list)
    return lng_list, lat_list, z_list

def update_figure(figure, lat_list, lng_list, z_list, mag_events, t_events):
    if(figure is not None):
        figure.data = []
    figure_df = pd.DataFrame({'lat': lat_list, 'lon': lng_list, 'z': z_list, 'mag': mag_events,
                              'time': t_events, 'size': [(mag_event**4) / 3.5 for mag_event in mag_events]})
    figure = px.scatter_mapbox(
        figure_df,
        lat="lat",
        lon="lon",
        hover_data=[
            "mag",
            "time",
            "lat",
            "lon"],
        size="size",
        color_discrete_sequence=["fuchsia"],
        zoom=map_zoom,
        height=300)
    figure = update_figure_layout(figure)
    return figure


def update_figure_with_cols(figure, col1, col2, lat_list, lng_list, z_list, mag_events, t_events):
    with col1:
        figure = update_figure(figure, lat_list, lng_list, z_list, mag_events, t_events)
    return figure


def tweepy_status_update(event_dict):
    if(len(event_dict) > 0):
        event = list(event_dict.values())[-1]
        print("tweepy_status_update (event): ", event)
        event_time = event['time']
        lng = lng_from_x(event['location'][0])
        lat = lat_from_y(event['location'][1])
        z = event['location'][2]
        mag = event['magnitude']
        bundle = (lng, lat, z, mag)
        global prev_event_bundle
        if(bundle != prev_event_bundle):
            print("----------New Event----------")
            prev_event_bundle = bundle
            if(mag > BOT_MAGNITUDE_THRESHOLD):
                print("time is %s, current time is %f" % (event_time, time.time()))
                print("Update status on twitter!")
                print("Magnitude %f earthquake happened at longitude %f, latitude %f at depth %f at time %s" % (mag, lng, lat, z, event_time))
                # get figure using update_figure
                figure = update_figure(None, [lat], [lng], [z], [mag], [event_time])
                print(figure)
                temp_time = time.time()
                figure.write_image("twitter_fig.png")
                print("time taken to render: %f"%(time.time() - temp_time))
    
                address = latlon2address(lat, lng, geolocator)

                if address is not None:
                    caption = f"Magnitude {mag} earthquake occurred at address {address} at time {event_time}"
                else:
                    caption = "Magnitude %f earthquake happened at longitude %f degrees, latitude %f degrees at depth %f km at time %s"%(mag, lng, lat, z, event_time)
                print("time taken to do a geopy API call: %f"%(time.time() - temp_time))
                api.update_with_media("twitter_fig.png", caption)
                print("time taken to upload to twitter: %f"%(time.time() - temp_time))
                #api.update_status("Magnitude %f earthquake happened at longitude %f, latitude %f at depth %f at time %s"%(mag, lng, lat, z, event_time))

def extract_df_from_event_dict(event_dict):
    event_dict_values = list(event_dict.values())
    event_dict_values.reverse()
    lat_values = []
    lon_values = []
    z_values = []
    mag_values = []
    time_values = []
    for event in event_dict_values:
        lon_values.append(lng_from_x(event['location'][0]))
        lat_values.append(lat_from_y(event['location'][1]))
        z_values.append(event['location'][2])
        mag_values.append(event['magnitude'])
        time_values.append(event['time'])
    event_dict_df = pd.DataFrame({'Magnitude': mag_values, 'Time': time_values, 'Latitude (deg)': lat_values, 'Longitude (deg)': lon_values, 'Depth (km)': z_values})
    return event_dict_df

# Page header
image_data = np.asarray(Image.open('quakeflow logo design 2.jpg'))
st.image(image_data, caption=None, width=None, use_column_width=None, clamp=False, channels='RGB', output_format='auto')
st.balloons()

# Streamlit layout
col1, col2 = st.beta_columns([2, 1])

# Initial plotting
with col1:
    experimental_df = pd.DataFrame({'lat': [], 'lon': [], 'z': [], 'mag': [], 'time': [], 'size': []})
    event_df = pd.DataFrame({'Magnitude': [], 'Time': [], 'Latitude (deg)': [], 'Longitude (deg)': [], 'Depth (km)': []})
    experimental = px.scatter_mapbox(
        experimental_df,
        lat="lat",
        lon="lon",
        hover_data=[
            "mag",
            "time",
            "lat",
            "lon"],
        color_discrete_sequence=["fuchsia"],
        zoom=map_zoom,
        height=300)
    experimental = update_figure_layout(experimental)
    map_figure_experimental = st.plotly_chart(experimental, width=map_width, height=map_height)

fig, (ax1) = plt.subplots(1, 1, figsize=(8, 5.8))
x = np.arange(window_length * window_number // hop_length) * (dt * hop_length)
ax1.set_ylim(-1, num_sta)
ax1.set_xlim(np.around(x[0]), np.around(x[-1]))

lines = []
for i in range(num_sta):
    line, = ax1.plot(x, np.zeros(len(x)) + i, linewidth=0.5)
    lines.append(line)
scatters = []
for i in range(num_sta):
    scatter = ax1.scatter([-1], [-1], s=300, c="white", marker="|")
    scatters.append(scatter)
ax1.scatter([-1], [-1], s=200, c="blue", marker="|", label="P-wave")
ax1.scatter([-1], [-1], s=200, c="red", marker="|", label="S-wave")
ax1.legend(loc="upper left")
ax1.title.set_text("Streaming Seismic Waveforms and Detected P/S Phases")

with col2:
    ui_plot = st.pyplot(plt)
    catalog_df_visual = st.empty()

prev_time = time.time()
prev_time_bot = time.time()

# Handle messages from Kafka
for i, message in enumerate(consumer):

    if message.topic == "waveform_raw":
        key = message.key
        timestamp = message.value[0]
        vec = message.value[1]
        wave_dict[key].append(message.value)
        wave_dict[key] = wave_dict[key][-window_number:]

    elif message.topic == "phasenet_picks":
        #print("phasenet!")
        key = message.key
        pick = message.value
        pick_dict[key].append(pick)

    elif message.topic == "gmma_events":
        # print("gmma!")
        key = np.round(timestamp_seconds(message.key) / event_min_gap) * event_min_gap
        event = message.value
        # event_list.extend(event)
        # event_dict[key].append(event)
        event_dict[key] = event
    else:
        raise("Topic Error!")

    # Tweepy timer
    if time.time() - prev_time_bot > event_min_gap:
        tweepy_status_update(event_dict)
        prev_time_bot = time.time()

    if time.time() - prev_time > refresh_sec:
        prev_time = time.time()

        keys = sorted(wave_dict.keys())
        print("refreshing...")

        min_t = prev_time
        max_t = 0
        # print("len(pick_dict): ", len(pick_dict))
        for j, k in enumerate(keys):
            tmp_vec = []
            tmp_t = []
            for _ in range(window_number - len(wave_dict[k])):
                tmp_vec.extend([[0] * 3] * window_length)
            for v in wave_dict[k]:
                tmp_vec.extend(v[1])
                tmp_t.append(v[0])

            lines[j].set_ydata(normalize(np.array(tmp_vec)[::hop_length, -1]) / 5 + j)
            if k in pick_dict:

                t0 = timestamp_seconds(max(tmp_t)) - window_length * (window_number - 1) * dt
                tn = timestamp_seconds(max(tmp_t)) + window_length * dt
                if tn > max_t:
                    max_t = tn
                if t0 < min_t:
                    min_t = t0
                t_picks, colors, t0_idx = get_plot_picks(pick_dict[k], t0, tn)
                scatters[j].set_offsets(np.c_[t_picks, np.ones_like(t_picks) * j])
                scatters[j].set_color(colors)

        if len(event_dict) > 0:
            t_events, mag_events, loc_events, t0_idx = get_plot_events(event_dict, min_t, max_t)
            if len(t_events) > 0:
                loc_events = np.array(loc_events)

                # organize data into the correct form
                lng_list, lat_list, z_list = loc_events_organize(loc_events)

                # update figure
                experimental = update_figure_with_cols(experimental, col1, col2, lat_list, lng_list, z_list, mag_events, t_events)
                event_df = extract_df_from_event_dict(event_dict)

        if len(keys) > 0:
            print("plotting...")
            with col2:
                ui_plot.pyplot(plt)
                catalog_df_visual.dataframe(event_df)
            with col1:
                map_figure_experimental.plotly_chart(experimental, width=map_width, height=map_height)

    if message.topic == "waveform_raw":
        time.sleep(refresh_sec / num_sta / 20)
