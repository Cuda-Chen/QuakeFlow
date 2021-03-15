FROM ubuntu:18.04

WORKDIR /APP

# Install Java and Spark
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y wget 
RUN wget https://apache.claz.org/kafka/2.7.0/kafka_2.13-2.7.0.tgz
RUN tar -xzf kafka_2.13-2.7.0.tgz

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y openjdk-8-jdk 

COPY . /APP

# Expose Zookeeper port
EXPOSE 9092

RUN echo "zookeeper.connect=quakeflow-zookeeper:2181" >> /APP/kafka_2.13-2.7.0/config/server.properties
RUN echo "listeners = PLAINTEXT://127.0.0.1:9092" >> /APP/kafka_2.13-2.7.0/config/server.properties
RUN echo "advertised.listeners = PLAINTEXT://127.0.0.1:9092" >> /APP/kafka_2.13-2.7.0/config/server.properties


CMD /APP/kafka_2.13-2.7.0/bin/kafka-server-start.sh /APP/kafka_2.13-2.7.0/config/server.properties

