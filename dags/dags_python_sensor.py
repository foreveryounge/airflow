import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.sensors.python import PythonSensor

with DAG(
    dag_id="dags_python_sensor",
    schedule="10 1 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    def check_api_update(http_conn_id, endpoint, base_dt_col, base_dt, **kwargs):
        import requests
        import json
        from datetime import timedelta

        connection = BaseHook.get_connection(http_conn_id)
        url = f"http://{connection.host}:{connection.port}/{endpoint}/1/100/{base_dt}"
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get("row")
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10]
        last_date = last_date.replace(".", "-").replace("/", "-")

        try:
            pendulum.from_format(last_date, "YYYY-MM-DD")
        except:
            from airflow.exceptions import AirflowException

            AirflowException(
                f"{base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다."
            )

        today_ymd = kwargs.get("data_interval_end").in_timezone("Asia/Seoul")
        update_ymd = (today_ymd - timedelta(days=1)).strftime("%Y-%m-%d")

        if last_date >= update_ymd:
            print(f"Update 완료 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})")
            return True
        else:
            print(
                f"Update 미완료 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})"
            )
            return False

    sensor_task = PythonSensor(
        task_id="sensor_task",
        python_callable=check_api_update,
        op_kwargs={
            "http_conn_id": "openapi.seoul.go.kr",
            "endpoint": "{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleRentUseDayInfo",
            "base_dt_col": "RENT_DT",
            "base_dt": "{{(data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds_nodash}}",
        },
        poke_interval=600,
        mode="reschedule",
    )
