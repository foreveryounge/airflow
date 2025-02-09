import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",  # DAG Name (파일명과 일치)
    schedule="0 0 * * *",  # 분, 시, 일, 월, 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # DAG이 언제 부터?
    catchup=False,  # 실제 현재 시점과 start_date 설정 시점 사이의 누락 구간 DAG 실행 여부 결정
    # params={"example_key": "example_value"}, task에 공통적으로 넘길 Parameter
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",  # 객체명과 일치
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",  # 객체명과 일치
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2  # Task 수행 순서
