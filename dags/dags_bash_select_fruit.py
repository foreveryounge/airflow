import pendulum
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2025, 2, 9, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="opt/airflow/plugins/shell/select_fruit.sh ORANGE",  # Task를 실행하는 주체는 Woker 컨테이너
    )

    t2_avocado = BashOperator(
        task_id="t1_avocado",
        bash_command="opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado
