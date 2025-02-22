from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

"""
서울시 공공데이터 API 추출 시, 특정 날짜 컬럼을 조사하여
배치 날짜 기준, 전날 데이터가 존재하는지 체크하는 센서
1. 데이터셋에 날짜 컬럼이 존재하고
2. API 사용 시, 그 날짜 컬럼을 기준으로 ORDER BY DESC 하여 데이터를 가져온다는 가정 하에 사용 가능
"""


class SeoulApiDateSensor(BaseSensorOperator):
    template_fields = ("endpoint", "base_dt")

    def __init__(self, dataset_nm, base_dt_col, base_dt=None, day_off=0, **kwargs):
        """
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (YYYY.mm.dd... or YYYY/mm/dd... 형태만 가능)
        day_off: 배치일 기준 데이터셋 생성 여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        """
        super().__init__(**kwargs)
        self.http_conn_id = "openapi.seoul.go.kr"
        self.endpoint = (
            "{{var.value.apikey_openapi_seoul_go_kr}}/json/" + dataset_nm + "/1/100"
        )
        self.base_dt_col = base_dt_col
        self.base_dt = base_dt
        self.day_off = day_off

    def poke(self, context):
        # Teamplate 변수를 이용해 꺼낼 수 있는 Variable이 정의되어 있음
        import requests
        import json
        from dateutil.relativedelta import relativedelta

        connection = BaseHook.get_connection(self.http_conn_id)
        url = (
            f"http://{connection.host}:{connection.port}/{self.endpoint}/{self.base_dt}"
        )
        self.log.info(f"request url:{url}")
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get("row")
        last_dt = row_data[0].get(self.base_dt_col)
        last_date = last_dt[:10]
        last_date = last_date.replace(".", "-").replace("/", "-")
        search_ymd = (
            context.get("data_interval_end").in_timezone("Asia/Seoul")
            + relativedelta(days=self.day_off)
        ).strftime("%Y-%m-%d")

        try:
            import pendulum

            pendulum.from_format(last_date, "YYYY-MM-DD")
        except:
            from airflow.exceptions import AirflowException

            AirflowException(
                f"{self.base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다."
            )

        if last_date >= search_ymd:
            self.log.info(
                f"Update 확인 (기준 날짜: {search_ymd} / API Last 날짜: {last_date})"
            )
            return True
        else:
            self.log.info(
                f"Update 미완료 (기준 날짜: {search_ymd} / API Last 날짜: {last_date})"
            )
            return False
