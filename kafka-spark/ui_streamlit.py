import streamlit as st
from collections import defaultdict
from kafka import KafkaConsumer
from json import loads
import time
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    key_deserializer=lambda x: loads(x.decode('utf-8')),
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

consumer.subscribe(['waveform_raw', 'phasenet_picks', 'gmma_events'])
# consumer.subscribe(['waveform_raw', 'phasenet_picks'])
# consumer.subscribe(['waveform_raw'])
# consumer.subscribe(['phasenet_picks'])

normalize = lambda x: (x - np.mean(x) + np.finfo(x.dtype).eps)/(np.std(x)+np.finfo(x.dtype).eps)
timestamp_seconds = lambda x: datetime.fromisoformat(x).timestamp()
# plot_scale_mag = lambda x, s1, s2: (x - 1) * (s2-s1) + 1
wave_dict = defaultdict(list)
pick_dict = defaultdict(list)
# event_list = []
event_dict = defaultdict(dict)
event_min_gap = 5
window_length = 100
window_number = 60
hop_length = 10
num_sta = 16
refresh_sec = 1.0
dt = 0.01


## 
# prev_time = time.time()
# num_plot = 0
# for i, message in enumerate(consumer):
    
#     key = message.key
#     timestamp = message.value[0]
#     vec = message.value[1]
#     t = datetime.fromisoformat(timestamp).timestamp() + np.arange(len(vec))*dt

#     wave_dict[key].append((t, vec))
#     wave_dict[key] = wave_dict[key][-window_number:]

#     if time.time() - prev_time > 5 :
#         prev_time = time.time()

#         keys = sorted(wave_dict.keys())
#         plot_data = {}
#         for i, k in enumerate(keys):
#             plot_data[k] = []
#             for j in range(window_number - len(wave_dict[k])):
#                 plot_data[k].extend([[0] * 3] * 100)
#             for v in wave_dict[k]:
#                 plot_data[k].extend(v[1])
#             plot_data[k] = normalize(np.array(plot_data[k])[::hop_length,-1])/8 + i

#         if num_plot == 0:
#             handle = st.line_chart(plot_data)
#         else:
#             handle.line_chart(plot_data)
#         num_plot += 1


### based on matplotlib
fig, (ax1, ax2)= plt.subplots(2, 1, gridspec_kw={"height_ratios": [1,0.8]}, figsize=(8, 12))
x = np.arange(window_length*window_number//hop_length) * (dt*hop_length)
ax1.set_ylim(-1, num_sta)
ax1.set_xlim(np.around(x[0]), np.around(x[-1]))

lines = []
for i in range(num_sta):
    # line, = ax1.plot(x, np.zeros(len(x)) + i, linewidth=0.5, color="gray")
    line, = ax1.plot(x, np.zeros(len(x)) + i, linewidth=0.5)
    lines.append(line)
scatters = []
for i in range(num_sta):
    scatter = ax1.scatter([-1], [-1], s=300, c="white", marker="|")
    # scatter = ax1.vlines([1], [i-0.5], [i+0.5], colors="black")
    scatters.append(scatter)
ax1.scatter([-1], [-1], s=200,c="blue", marker="|", label="P-wave")
ax1.scatter([-1], [-1], s=200, c="red", marker="|", label="S-wave")
ax1.legend(loc="upper left")
ax1.title.set_text("Streaming Seismic Waveforms and Detected P/S Phases")

ax2.axis("scaled")
ax2.set_xlabel("x(km)")
ax2.set_ylabel("y(km)")
ax2.set_ylim([50, 80])
ax2.set_xlim([30, 60])
scatter_events = ax2.scatter([-1], [-1], s=120,  c="r", marker="o", label="Earthquakes")
ax2.legend(loc="upper left")
ax2.title.set_text("Associated Earthquakes")

ui_plot = st.pyplot(plt)

cities = []
scale = 5000
fig_temp = go.Figure()

map_figure = st.plotly_chart(fig_temp)



# plt.show()

# prev_time = time.time()
# for i, message in enumerate(consumer):

#     # print(i, message.key)
    
#     key = message.key
#     timestamp = message.value[0]
#     vec = message.value[1]
#     t = datetime.fromisoformat(timestamp).timestamp() + np.arange(len(vec))*dt

#     wave_dict[key].append((t, vec))
#     wave_dict[key] = wave_dict[key][-window_number:]

#     if time.time() - prev_time > refresh_sec:
#         prev_time = time.time()

#         keys = sorted(wave_dict.keys())
#         for i, k in enumerate(keys):
#             tmp = []
#             # if len(wave_dict[k]) < window_number:
#             #     print(wave_dict[k])
#             for j in range(window_number - len(wave_dict[k])):
#                 tmp.extend([[0] * 3] * window_length)
#             for v in wave_dict[k]:
#                 tmp.extend(v[1])
#             lines[i].set_ydata(normalize(np.array(tmp)[::hop_length,-1])/8 + i)
        
#         ui_plot.pyplot(plt)

#     time.sleep(1.0/num_sta)


prev_time = time.time()

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
            if timestamp_seconds(x["time"]) <= tn - 8 :
                t_events.append(timestamp_seconds(x["time"]) - t0)
                mag_events.append(x["magnitude"])
                loc_events.append(x["location"])
            else:
                return t_events, mag_events, loc_events, t0_idx 
    return t_events, mag_events, loc_events, t0_idx 

def lng_from_x(x):
    lng = (1/111.1666666667) * x - 116.0304497751
    return lng

def lat_from_y(y):
    lat = (1/111.1537242472) * y + 32.4800184066
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

for i, message in enumerate(consumer):

    if message.topic == "waveform_raw":
        key = message.key
        timestamp = message.value[0]
        vec = message.value[1]
        wave_dict[key].append(message.value)
        wave_dict[key] = wave_dict[key][-window_number:]
    
    elif message.topic == "phasenet_picks":
        # print("phasenet!")
        key = message.key
        pick = message.value
        pick_dict[key].append(pick)

    elif message.topic == "gmma_events":
        # print("gmma!")
        key = np.round(timestamp_seconds(message.key)/event_min_gap) * event_min_gap
        event = message.value
        # event_list.extend(event)
        # event_dict[key].append(event)
        event_dict[key] = event
    else:
        raise("Topic Error!")

    if time.time() - prev_time > refresh_sec:
        prev_time = time.time()

        keys = sorted(wave_dict.keys())
        print("refreshing...")
        
        min_t = prev_time
        max_t = 0
        print("len(pick_dict): ", len(pick_dict))
        for j, k in enumerate(keys):
            tmp_vec = []
            tmp_t = []
            for _ in range(window_number - len(wave_dict[k])):
                tmp_vec.extend([[0] * 3] * window_length)
            for v in wave_dict[k]:
                tmp_vec.extend(v[1])
                tmp_t.append(v[0])

            lines[j].set_ydata(normalize(np.array(tmp_vec)[::hop_length,-1])/5 + j)
            if k in pick_dict:

                t0 = timestamp_seconds(max(tmp_t)) - window_length * (window_number-1) * dt
                tn = timestamp_seconds(max(tmp_t)) + window_length * dt
                if tn > max_t:
                    max_t = tn
                if t0 < min_t:
                    min_t = t0
                t_picks, colors, t0_idx = get_plot_picks(pick_dict[k], t0, tn)
                # pick_dict[k] = pick_dict[k][t0_idx:]
                scatters[j].set_offsets(np.c_[t_picks, np.ones_like(t_picks)*j])
                # scatters[j].set_edgecolors(colors)
                # scatters[j].set_facecolors(colors)
                scatters[j].set_color(colors)
        print("len(event_dict): ", len(event_dict))
        if len(event_dict) > 0:
            t_events, mag_events, loc_events, t0_idx = get_plot_events(event_dict, min_t, max_t)
            print("loc_events: ", loc_events)
            print("mag_events: ", mag_events)
            print("t_events: ", t_events)
            print("t0_idx: ", t0_idx)
            if len(t_events) > 0:
                loc_events = np.array(loc_events)
                scatter_events.set_offsets(loc_events[:,:2])
                scatter_events.set_sizes(10**np.array(mag_events)*10)
                alpha = np.array(t_events)/(window_length*(window_number+1)*dt)
                red = np.zeros([len(alpha),3])
                red[:,0] = 1.0
                rgba = np.hstack([red, alpha[:,np.newaxis]])
                scatter_events.set_color(np.clip(rgba, 0, 1))

                #insert plotly code here
                #reset plot (there seems to be no way to keep track of traces one by one so this will have to do)
                fig_temp.data = []
                # organize data into the correct form
                lng_list, lat_list, z_list = loc_events_organize(loc_events)
                # for i in range(len(lng_list)):
                #     fig_temp.add_trace(go.Scattergeo(
                #         locationmode = 'USA-states',
                #         lon = [lng_list[i]],
                #         lat = [lat_list[i]],
                #         text = "time: %f\nmagnitude: %f"%(t_events[i], mag_events[i]),
                #         marker = dict(
                #             size = [max(20.0 ** mag_events[i], 10)],
                #             color = 'royalblue',
                #             line_color='rgb(40,40,40)',
                #             line_width=0.5,
                #             sizemode = 'area'
                #         )
                #     ))
                #     print("marker size: ", [max(20.0 ** mag_events[i], 10)])
                
                # UNCOMMENT OUT THIS LINE TO SEE A SIZE 6 EARTHQUAKE
                # mag_events[-1] = 6
                fig_temp.add_trace(go.Scattergeo(
                    locationmode = 'USA-states',
                    lon = lng_list,
                    lat = lat_list,
                    text = ["time: %f\nmagnitude: %f"%(t_events[i], mag_events[i]) for i in range(len(lng_list))],
                    marker = dict(
                        size = [(mag_event**4) * 10 for mag_event in mag_events],
                        color = 'royalblue',
                        line_color='rgb(40,40,40)',
                        line_width=0.5,
                        sizemode = 'area'
                    )
                ))
                fig_temp.update_layout(
                        title_text = 'test',
                        showlegend = True,
                        width=1000,
                        height=1000,
                        geo = dict(
                            landcolor = 'rgb(217, 217, 217)',
                            lonaxis = dict(
                                showgrid = True,
                                gridwidth = 0.05,
                                range= [ -116.0304497751, -115.0304497751 ],
                                dtick = 5
                            ),
                            lataxis = dict (
                                showgrid = True,
                                gridwidth = 0.05,
                                range= [ 32.4800184066, 33.4800184066],
                                dtick = 5
                            )
                        )
                    )
                fig_temp.update_geos(
                    resolution=50,
                    showcoastlines=True, coastlinecolor="RebeccaPurple",
                    showland=True, landcolor="LightGreen",
                    showocean=True, oceancolor="LightBlue",
                    showlakes=True, lakecolor="Blue",
                    showrivers=True, rivercolor="Blue"
                )
                #Uncomment if you want a zoomed-in plot
                #fig_temp.update_geos(fitbounds="locations")


        if len(keys) > 0:
            print("plotting...")
            ui_plot.pyplot(plt)
            # insert plotly code here
            map_figure.plotly_chart(fig_temp)

    if message.topic == "waveform_raw":
        time.sleep(refresh_sec/num_sta/20)