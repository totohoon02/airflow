from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

dag = DAG(
    dag_id="05_01_xcom_dag"
)


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
