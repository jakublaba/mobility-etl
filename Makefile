IMAGES=mobility-etl-airflow-init mobility-etl-airflow-scheduler mobility-etl-airflow-webserver

init:
	docker compose up airflow-init -d

start: init
	docker compose up -d

stop:
	docker compose down --remove-orphans

restart: stop start

clean: stop
	docker image rm $(IMAGES)

.PHONY: init start stop restart clean

