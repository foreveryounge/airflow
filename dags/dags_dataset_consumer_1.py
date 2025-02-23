import pendulum
from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
    dag_id="dags_dataset_consumer_1",
    schedule=[dataset_dags_dataset_producer_1],
    # DB에 저장되어 있는 Key 값으로 Subscribe
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo {{ ti.run_id }} && echo 'producer_1 이 완료되면 수행'",
    )
