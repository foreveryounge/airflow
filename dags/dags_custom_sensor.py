import pendulum
from airflow import DAG
from sensors.seoul_api_date_sensor import SeoulApiDateSensor

with DAG(
    dag_id="dags_custom_sensor",
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    tb_cycle_rent_use_dayinfo_sensor = SeoulApiDateSensor(
        task_id="tb_cycle_rent_use_dayinfo_sensor",
        dataset_nm="tbCycleRentUseDayInfo",
        base_dt_col="RENT_DT",
        base_dt="{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds_nodash }}",
        day_off=-1,
        poke_interval=600,
        mode="reschedule",
    )

    tb_cycle_failure_report_sensor = SeoulApiDateSensor(
        task_id="tb_cycle_failure_report_sensor",
        dataset_nm="tbCycleFailureReport",
        base_dt_col="regDttm",
        base_dt="{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds_nodash }}",
        day_off=-1,
        poke_interval=600,
        mode="reschedule",
    )
