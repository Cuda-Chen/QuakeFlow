FROM ubuntu:18.04

# Install Java and Spark
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y openjdk-8-jdk git wget
RUN wget "https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz"
RUN tar -xzvf spark-2.4.7-bin-hadoop2.7.tgz
RUN rm spark-2.4.7-bin-hadoop2.7.tgz

# Install Python3.7
RUN apt-get update
RUN apt-get install -y curl python3.7 python3.7-dev python3.7-distutils
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

# Set python 3 as the default python
RUN update-alternatives --set python /usr/bin/python3.7

# Upgrade pip to latest version
RUN curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py

# Setup env variables
ENV PATH="$PATH:/usr/bin/python"
ENV PYTHONPATH="$PYTHONPATH:/usr/bin/python"
ENV PYSPARK_PYTHON=/usr/bin/python


WORKDIR /app
COPY requirements.txt /app
RUN pip3 install -r requirements.txt

COPY . /app
CMD /spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 /app/spark.py