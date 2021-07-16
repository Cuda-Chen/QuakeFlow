FROM continuumio/miniconda3

WORKDIR /app

# Create the environment:
COPY env.yml /app
RUN conda env create --name cs329s --file=env.yml
# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "cs329s", "/bin/bash", "-c"]

# Copy files
COPY . /app

# Expose API port
EXPOSE 8005

ENV PYTHONUNBUFFERED=1

# Start API server
# ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "cs329s", "python", "app_plotly.py"]
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "cs329s", "gunicorn", "app_plotly:server", "--reload", "-b", "0.0.0.0:8005"]
