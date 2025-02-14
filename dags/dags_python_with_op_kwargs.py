import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2

tz = pendulum.timezone(tz="Asia/Seoul")

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz=tz),
    catchup=False,
) as dag:

    regist2_t1 = PythonOperator(
        task_id="regist2_t1",
        python_callable=regist2,
        op_args=["wyjang", "woman", "kr", "seoul"],
        kwargs={"email": "for_everyoung@gmail.com", "phone": "010"},
    )

    regist2_t1
