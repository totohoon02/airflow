import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# dag = DAG(
#     dag_id="01_unscheduled",
#     start_date=dt.datetime(2019, 1, 1),
#     schedule_interval=None
# )

dag = DAG(
    dag_id="02_scheduled",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    schedule_interval=dt.timedelta(days=3),  # 인터벌 지정
    catchup=False # 백필 사용 안함
)

fetch_events = BashOperator(
    task_id='fetch_events',
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/event{{ds}}.json"  # 데이터 파티셔닝
        "https://localhost:5000/events?"
        "start_date={{execution_date.strftime('%Y-%m-%d')}}"  # 실행일 부터  ds
        "&end_date={{next_execution_date.strftime('%Y-%m-%d')}}"  # 다음 실행일 까지 next_ds : YYYY-MM-DD 형식
    ),
    dag=dag
)


def _calculate_stats(**context):
    """ 이벤트 통계 계산"""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv"
    },
    dag=dag
)

fetch_events >> calculate_stats
