import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.models import Variable

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(",")]

with DAG(
    dag_id="dags_email_on_failure",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        "email": email_list,
        "email_on_failure": True,
        # 하위 Task 각각에 개별 적용
    },
) as dag:

    @task(task_id="python_fail")
    def python_task_func():
        from airflow.exceptions import AirflowException

        raise AirflowException("에러 발생")

    python_task_func

    bash_fail = BashOperator(
        task_id="bash_fail",
        bash_command="exit 1",
    )

    bash_success = BashOperator(
        task_id="bash_success",
        bash_command="exit 0",
    )

    # flow 미설정 시, 각 Task 병렬 실행
