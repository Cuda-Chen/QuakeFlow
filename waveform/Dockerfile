FROM continuumio/miniconda3

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Create the environment:
COPY env.yml /app
RUN conda env create --name cs329s --file=env.yml
# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "cs329s", "/bin/bash", "-c"]

# Copy files
COPY . /app

# Start API server
# ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "cs329s", "python", "producer.py"]
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "cs329s", "python", "producer_iris.py"]