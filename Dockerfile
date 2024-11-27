FROM apache/airflow:slim-latest-python3.12

USER root
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

USER airflow
RUN pip install psycopg2-binary

COPY .env /opt/airflow
COPY requirements.txt .
RUN pip install -r requirements.txt
