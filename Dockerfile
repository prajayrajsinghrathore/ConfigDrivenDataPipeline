FROM apache/airflow:3.0.2

# Switch to airflow user before installing (recommended)
USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libkrb5-dev \
    libgssapi-krb5-2 \
    krb5-config \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/airflow/config/schemas \
    && mkdir -p /opt/airflow/config/data_sources \
    && mkdir -p /tmp/airflow_output

USER airflow

# Copy and install requirements
COPY requirements.txt /requirements.txt

# Install extra Python packages
RUN pip install -r /requirements.txt
#RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to airflow user (very important!)
USER airflow
