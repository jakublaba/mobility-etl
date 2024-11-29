IMAGES=mobility-etl-airflow-init mobility-etl-airflow-scheduler mobility-etl-airflow-webserver
SPARK_WORKERS=2

init:
	docker compose up airflow-init -d

start: init
	docker compose up --scale spark-worker=$(SPARK_WORKERS) -d

stop:
	docker compose down --volumes --remove-orphans

restart: stop start

clean: stop
	docker image rm $(IMAGES)

.PHONY: init start stop restart clean

