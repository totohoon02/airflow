# Airflow

---

## Airflow

- 파이썬 코드를 이용한 유연한 파이프라인 정의
- 스케줄링
- 모니터링과 실패 처리
- 실시간 데이터에는 적합하지 않다

### 데이터 파이프라인

- 원하는 결과를 얻기 위한 여러 태스크 또는 동작

  - 실시간 날씨
    1. 다른 시스템의 API를 통해 일기 예보 데이터 가져오기
    2. 데이터 정제
    3. 변환된 데이터를 전송
    - 각 태스크는 정해진 순서가 있다.

- 태스크 그래프
  - 방향성 비순환 그래프(DAG)
  - 태스크 및 의존성 표시
  - 병렬 실행이 가능, 효율적 리소스 활용
    - 날씨 데이터 → 정제
    - 판메 데이터 → 정제
      - 데이터 세트 결합

## Installaiton

### Get Sample Data

```python
https://ll.thespacedevs.com/2.0.0/launch/upcoming
```

### Airflow 설치

```bash
# project 경로에서 **NOT AT ROOT DIR**
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.0/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
docker compose up -d
```

### Virtual Environment

```bash
# use 'venv'
python -m venv .venv
. .venv/bin/activate

pip install apache-airflow
```

- Sample Code

```python
import json
import pathlib

import airflow
import airflow.utils
import airflow.utils.dates
import requests
import requests.exceptions as request_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="""
        curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'""",
    dag=dag
)

def _get_pictures():
    pathlib.Path("tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launche['image'] for launce in launches['results']]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, 'wb') as f:
                    f.write(response.content)
                print(f"Download {image_url} to {target_file}")
            except request_exceptions.MissingSchema:
                print(f"{image_url}: invalid url")
            except request_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images"',
    dag=dag
)

download_launches >> get_pictures >> notify

```

- start_date부터 필요한 실행 시점까지 자동 실행
- 실패 시 실패한 태스크 Clear하면 해당 태스크부터 자동 실행된다.

## Airflow 기능

### 스케줄링

```python
dag = DAG(
    dag_id="download_rocket_launches",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    schedule_interval="@daily", # 매일 1번 실행 / 크론식으로도 표현 가능
)
```

```python
fetch_events = BashOperator(
    task_id='fetch_events',
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/event{{ds}}.json"  # 데이터 파티셔닝
        "https://localhost:5000/events?"
        "start_date={{execution_date.strftime('%Y-%m-%d')}}"
        "&end_date={{next_execution_date.strftime('%Y-%m-%d')}}"
    ),
    dag=dag
)

# 동일 표현
fetch_events = BashOperator(
    task_id='fetch_events',
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/event{{ds}}.json"  # 데이터 파티셔닝
        "https://localhost:5000/events?"
        "start_date={{ds}}"
        "&end_date={{next_ds}}"
    ),
    dag=dag
)

```

- ds는 시작 날짜

태스크 디자인

- 원자성
  - 모든 작업이 성공한 후에 다음 태스크 진행
- 멱등성
  - 반복 시 동일한 결과 유지

### 태스크 콘텍스트, 템플릿

```python

# Bash Operator
get_data_bash = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"  # jinja 템플릿 지정
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag
)
```

- 파이썬 오퍼레이터 사용 시

```python
def _get_data_py(output_path, execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    hour = (datetime.now() - timedelta(hours=1)).hour

		url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data_py,
    op_kwargs={"output_path": "/tmp/wikipageviews.gz"}, # 파라미터 추가
    dag=None # Dag에 등록하지 않으면 실행x
)

def _print_context(data_interval_start, **context):
    start=context["execution_date"]
    print("start", start)
    print("명시한 컨텍스트는 kwargs에 잡히지 않아요", data_interval_start)

    print(context)

print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag
)
```

- output_path : op_kwargs에서 딕셔너리 형태로 입력 가능
- op_args 리스트 형태, 순서에 영향 있을 듯
- 태스크 콘텍스트에서 execution_date를 받아 올 수 있다.

### 데이터 저장

- Xcom
- 외부 데이터베이스

```bash
pip install apache-airflow-providers-postgres
```

테이블 생성, 작성

```python
create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="my_postgres",
    sql="CREATE TABLE IF NOT EXISTS pageview_counts ("
        " pagename VARCHAR(255) NOT NULL,"
        " pageviewcount INT NOT NULL,"
        " datetime TIMESTAMP NOT NULL"
        ");",
    dag=dag
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag
)

# 도커에서 postgre 접속
# psql -U airflow -d airflow
# host = postgres / 컴포즈 파일에서 이름으로 찾는듯?

get_data >> extract_gz >> fetch_pageviews >> create_table >> write_to_postgres
```

- CLI 혹은 Airflow에서 Connection 추가

![image.png](./readme_images/image.png)

- 이렇게 하면
  1. Wiki에서 데이터 받아와서
  2. 압축 해제
  3. 딕셔너리로 만들고 sql 문 작성
  4. Postgres 테이블 생성
  5. 테이블에 데이터 쓰기 까지 자동화

## 태스크 의존성 정의

- 이전 장 까지 선형적 의존성 사용

### 선형 체인 의존성 - 팬인, 팬아웃 구조

![image.png](/readme_images/image%201.png)

```python
start >> [print1_1, print2_1]
print1_1 >> print1_2
print2_1 >> print2_2
[print1_2, print2_2] >> print3
```

### 브랜치

![image.png](/readme_images/image%202.png)

```python
def _pick_branch(stage):
    if is_use_new:
        return f"print_{stage}_new"
    else:
        return f"print_{stage}_old"


print_1_branch = BranchPythonOperator(
    task_id="pick_branch_1",
    python_callable=_pick_branch,
    op_kwargs={"stage": "1"},
    dag=dag
)

start >> print_1_branch >> [print_1_old, print_1_new] >> print_3
```

- 의존 관계가 브랜치에서 들어간다.
- 오퍼레이션 트리거 룰 기본 값 = All_Success
  `trigger_rule="none_failed"` 을 추가해서 skip 발생해도 다음 태스크 실행하게 변경
- print*2*\*와 print3사이에 중간 태스크를 넣으면 print3에 none_failed 넣을 필요가 없다.

### 태스크 간 데이터 공유

1. xcom

```python
def _set_xcom_var(**context):
    context["task_instance"].xcom_push(key="my_xcom_var", value=240912)

set_xcom_var = PythonOperator(
    task_id="set_xcom_val",
    python_callable=_set_xcom_var,
    dag=dag
)

def _get_xcom_var(**context):
    my_xcom_value = context["task_instance"].xcom_pull(task_ids="set_xcom_val", key="my_xcom_var")
    print("my_xcom_value", my_xcom_value)

get_xcom_var = PythonOperator(
    task_id="get_xcom_val",
    python_callable=_get_xcom_var,
    dag=dag
)

set_xcom_var >> get_xcom_var

```

- task_id와 key를 이용해 데이터를 받아온다.
- 작은 데이터만 사용 가능

### 데코레이터 기반 태스크

```python
@dag(
    dag_id="05_02_deco_dag"
)
def task_builder():
    @task()
    def start():
        start_time = datetime.now()
        return start_time

    @task(task_id="set_deco")
    def set_decorator():
        deco_val = "Hello from decorator"
        return deco_val

    @task(task_id="print_deco_old")
    def print_decorator_old(deco_val: str):
        print(f"var form xcom: {deco_val}")

    @task(task_id="print_deco_new")
    def print_decorator_new(deco_val: str):
        print(f"new enw enw var form xcom: {deco_val}")

    @task.branch()
    def branch_print_deco(start_time: datetime):
        print(f"started_at: {start_time}")
        is_use_new = True
        if is_use_new:
            return "print_deco_new"
        else:
            return "print_deco_old"

    @task()
    def dummy1():

        # 의도적 오류 띄우기
        raise Exception("foo")
        dummy_val = "this is dummy"
        return dummy_val

    @task(trigger_rule="always")
    def dummy2(start_time: datetime, dummy_val: str):
        print(f"started_at: {start_time}")
        print(dummy_val)

    @task(trigger_rule="none_failed")
    def end():
        pass

    start_task = start()
    deco_task = set_decorator()

    # graph
    start_task >> branch_print_deco(start_task) >> [print_decorator_old(deco_task),
                                                    print_decorator_new(deco_task)] >> end()
    start_task >> dummy2(start_task, dummy1()) >> end()

task_builder()
```

- start()를 매번 호출하면 그래프에 각각 task가 생긴다
- @task() 안에 옵션 줄 수 있다.
- 리턴 값은 알아서 xcom에 들어간다
- 반환 값이랑 태스크 주는게 조금 이상하긴 한데 저렇게 하면 된다.

## 워크플로 트리거

### 태스크의 결과에 따라서 트리깅

```python
@dag(
    dag_id="06_02_file_sensor",
    concurrency=50,  # 동시 실행 태스크 수 제한
)
def task_builder():
    @task()
    def start():
        pass

    @task.sensor(poke_interval=5)
    def wait_for_data(data_id):
        file_path = Path("/tmp/data/data2.txt")
        return file_path.exists()

    @task()
    def end():
        pass

    start() >> sensor_task >> end()

task_builder()
```

- `@task.sensor`에서 `data2.txt`가 존재할 때 까지 5초마다 poke를 보낸다.
- `data2.txt`가 존재하면 다음 태스크로 진행
- `/tmp`→ 기본 디렉터리 tmp 아님! `cd /tmp` 로 들어가야함

### 다른 DAG를 트리거하기

```python

trigger_another_dag = TriggerDagRunOperator(
    task_id="run_task2",
    trigger_dag_id="06_02_01_another_dag",
)

start() >> sensor_task >> end() >> trigger_another_dag

# other file
@dag(
    dag_id="06_02_01_another_dag",
)
def task_builder2():
    @task()
    def start():
        pass

    @task()
    def hello():
        print("world")

    @task()
    def end():
        pass

    start() >> hello() >> end()

task_builder2()

```

- dag_id로 다른 DAG를 실행한다.
- 타겟 DAG가 활성화 되어있어야 한다.
