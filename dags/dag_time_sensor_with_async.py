import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dags_time_sensor_with_async",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2025, 2, 1, 0, 0, 0),
    end_date=pendulum.datetime(2025, 2, 1, 1, 0, 0),
    catchup=True,
) as dag:

    sync_sensor = DateTimeSensorAsync(
        # 설정한 목표 시간까지 기다리는 Sensor
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )
