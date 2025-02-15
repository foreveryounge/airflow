import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_trigger_eg2",
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task.branch(task_id="branching")
    def random_branch():
        import random

        item_list = ["A", "B", "C"]
        selected_item = random.choice(item_list)

        if selected_item == "A":
            return "task_a"
        elif selected_item == "B":
            return "task_b"
        else:
            return "task_c"

    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo upstream1",
    )

    @task(task_id="task_b")
    def task_b():
        print("정상 처리")

    @task(task_id="task_c")
    def task_c():
        print("정상 처리")

    @task(task_id="task_d", trigger_rele="none_skipped")
    def task_d():
        print("정상 처리")

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()
