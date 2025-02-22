import pendulum
from airflow import DAG
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="dags_file_sensor",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    tbCycleFailureReport_sensor = FileSensor(
        task_id="tbCycleFailureReport_sensor",
        fs_conn_id="conn_file_opt_airflow_files",
        filepath="tbCycleFailureReport/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}/tbCycleFailureReport.csv",
        recursive=False,
        poke_interval=60,
        timeout=60 * 60 * 24,  # 1일
        mode="reschedule",
    )
