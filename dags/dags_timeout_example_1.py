import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(",")]

with DAG(
    dag_id="dags_timeout_example_1",
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        "execution_timeout": timedelta(seconds=20),
        "email": email_list,
        "email_on_failure": True,
    },
) as dag:

    bash_sleep_30 = BashOperator(
        task_id="bash_sleep_30",
        bash_command="sleep 30",
    )

    bash_sleep_10 = BashOperator(
        trigger_rule="all_done",
        # 상위 Task 실패 시, 하위 Task 실행하기 위한 설정
        task_id="bash_sleep_10",
        bash_command="sleep 10",
    )

    bash_sleep_30 >> bash_sleep_10
