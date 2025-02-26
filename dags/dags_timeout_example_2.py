import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(",")]

with DAG(
    dag_id="dags_timeout_example_2",
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        "execution_timeout": timedelta(seconds=40),
        "email": email_list,
        "email_on_failure": True,
    },
) as dag:

    bash_sleep_35 = BashOperator(
        task_id="bash_sleep_35",
        bash_command="sleep 35",
    )

    bash_sleep_36 = BashOperator(
        trigger_rule="all_done",
        # 상위 Task 실패 시, 하위 Task 실행하기 위한 설정
        task_id="bash_sleep_36",
        bash_command="sleep 36",
    )

    bash_go = BashOperator(
        task_id="bash_go",
        bash_command="exit 0",
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go
