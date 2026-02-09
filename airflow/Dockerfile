FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy Requirements
COPY requirements.txt /requirements.txt

# install Python Packages
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGS
COPY dags/ ${AIRFLOW_HOME}/dags/

# Copy plugins if any
COPY plugins/ ${AIRFLOW_HOME}/plugins/