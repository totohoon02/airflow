from datetime import datetime, timedelta
from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="03_wiki_dag",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp"
)


def _print_context(data_interval_start, **context):
    start = context["execution_date"]
    print("start", start)
    print("명시한 컨텍스트는 kwargs에 잡히지 않아요", data_interval_start)
    print(context)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=None
)


# get_data_bash = BashOperator(
#     task_id="get_data",
#     bash_command=(
#         "curl -o /tmp/wikipageviews.gz "
#         "https://dumps.wikimedia.org/other/pageviews/"
#         "{{ execution_date.year }}/"  # jinja 템플릿 지정
#         "{{ execution_date.year }}-"
#         "{{ '{:02}'.format(execution_date.month) }}/"
#         "pageviews-{{ execution_date.year }}"
#         "{{ '{:02}'.format(execution_date.month) }}"
#         "{{ '{:02}'.format(execution_date.day) }}-"
#         "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
#     ),
#     dag=dag
# )


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
    op_kwargs={"output_path": "/tmp/wikipageviews.gz"},
    dag=dag
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag
)


def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", 'r') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", 'w') as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', '{pageviewcount}', '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag
)

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

get_data >> extract_gz >> fetch_pageviews >> create_table >> write_to_postgres
