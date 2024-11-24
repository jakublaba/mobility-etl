FROM apache/airflow:slim-latest-python3.12

USER root
RUN apt-get update && apt-get install -y libpq-dev

USER airflow
RUN pip install psycopg2-binary

WORKDIR /sources
COPY requirements.txt .
RUN pip install -r requirements.txt
