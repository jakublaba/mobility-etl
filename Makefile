IMAGES=pad-etl-airflow-init pad-etl-airflow-scheduler pad-etl-airflow-webserver

init:
	docker compose up airflow-init -d

start: init
	docker compose up -d

stop:
	docker compose down --volumes --remove-orphans

restart: stop start

clean: stop
	docker image rm $(IMAGES)

.PHONY: init start stop restart clean

