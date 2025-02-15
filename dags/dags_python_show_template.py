import pendulum
import datetime
from pprint import pprint
from airflow import DAG
from airflow.decorators import task

tz = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="dags_python_show_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 10, tz=tz),
    catchup=True,
) as dag:

    @task(task_id="python_task")
    def show_template(**kwargs):
        pprint(kwargs)

    show_template()
