import pendulum
from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
    dag_id="dags_dataset_producer_1",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_task = BashOperator(
        task_id="bash_task",
        outlets=[dataset_dags_dataset_producer_1],
        # BaseOperator에 정의되어 있는 Parameter
        # DAG 내의 Task에 의한 Publish
        bash_command="echo 'producer_1 수행 완료'",
    )
