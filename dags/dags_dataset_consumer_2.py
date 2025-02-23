import pendulum
from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")
dataset_dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id="dags_dataset_consumer_2",
    schedule=[dataset_dags_dataset_producer_1, dataset_dags_dataset_producer_2],
    # DB에 저장되어 있는 Key 값으로 Subscribe
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo {{ ti.run_id }} && echo 'producer_1 와 producer_2 가 완료되면 수행'",
    )
