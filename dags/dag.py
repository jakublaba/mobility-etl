from datetime import datetime

from airflow.decorators import dag, task

@dag(
    dag_id="proof-of-concept-dag",
    schedule="* * * * *", # every minute
    start_date=datetime(2024, 11, 24, 0, 0),
    catchup=False,
)
def pad_etl_dag():
    @task
    def t1():
        print("Executed Task 1")

    @task
    def t2():
        print("Executed Task 2")

    @task
    def t3():
        print("Executed Task 3")

    @task
    def t4():
        print("Executed Task 4")

    t1() >> [t2(), t3()] >> t4()

pad_etl_dag()
