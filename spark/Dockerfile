FROM ubuntu:18.04

# Install Java and Spark
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y openjdk-8-jdk git wget tzdata
RUN ln -fs /usr/share/zoneinfo/US/Pacific-New /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

# RUN wget "https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz"
# RUN tar -xzvf spark-2.4.7-bin-hadoop2.7.tgz
# RUN rm spark-2.4.7-bin-hadoop2.7.tgz

# Install Python3.7
# RUN apt-get update
# RUN apt-get install -y curl python3.7 python3.7-dev python3.7-distutils
# RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

# # Set python 3 as the default python
# RUN update-alternatives --set python /usr/bin/python3.7

# # Upgrade pip to latest version
# RUN curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
#     python get-pip.py --force-reinstall && \
#     rm get-pip.py


ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN apt-get update

RUN apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 
RUN conda --version

# Setup env variables
ENV PYTHONUNBUFFERED=1
# ENV PATH="$PATH:/usr/bin/python"
# ENV PYTHONPATH="$PYTHONPATH:/usr/bin/python"
# ENV PYSPARK_PYTHON=/usr/bin/python

WORKDIR /app
COPY env.yml /app
RUN conda env create --name cs329s --file=env.yml
SHELL ["conda", "run", "-n", "cs329s", "/bin/bash", "-c"]

# COPY requirements.txt /app
# RUN pip3 install -r requirements.txt


COPY . /app
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "cs329s", "spark-submit", "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=PST", \
    "--conf", "spark.executor.extraJavaOptions=-Duser.timezone=PST", \
    "--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3", "/app/spark.py"]
# CMD /spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 /app/spark.py

# https://stackoverflow.com/questions/59525481/using-spark-streaming-from-pyspark-2-4-4