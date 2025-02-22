import pendulum
from airflow import DAG
from operators.seoul_api_to_csv_operators import SeoulApiToCsvOperator

with DAG(
    dag_id="dags_seoul_api_corona",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    """서울시 공공자전거 이용정보(일별)"""
    tb_cycle_rent_use_dayinfo = SeoulApiToCsvOperator(
        task_id="tb_cycle_rent_use_dayinfo",
        dataset_nm="tbCycleRentUseDayInfo",
        path="/opt/airflow/files/tbCycleRentUseDayInfo/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name="tbCycleRentUseDayInfo.csv",
        base_dt="{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
    )

    """서울시 공공자전거 고장신고 내역"""
    tb_cycle_failure_report = SeoulApiToCsvOperator(
        task_id="tb_cycle_failure_report",
        dataset_nm="tbCycleFailureReport",
        path="/opt/airflow/files/tbCycleFailureReport/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name="tbCycleFailureReport.csv",
        base_dt="{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
    )

    tb_cycle_rent_use_dayinfo >> tb_cycle_failure_report
