from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id="04_01_fan_in_out_dag",
)


def _print(stage):
    if stage == "2_1":
        raise Exception("2_1")
    print(stage)


start = EmptyOperator(
    task_id="dummy",
    dag=dag
)

print1_1 = PythonOperator(
    task_id="print1_1",
    python_callable=_print,
    op_kwargs={"stage": "1_1"},
    dag=dag
)

print1_2 = PythonOperator(
    task_id="print1_2",
    python_callable=_print,
    op_kwargs={"stage": "1_2"},
    dag=dag
)

print2_1 = PythonOperator(
    task_id="print2_1",
    python_callable=_print,
    op_kwargs={"stage": "2_1"},
    dag=dag
)

print2_2 = PythonOperator(
    task_id="print2_2",
    python_callable=_print,
    op_kwargs={"stage": "2_2"},
    dag=dag
)

print3 = PythonOperator(
    task_id="print3",
    python_callable=_print,
    op_kwargs={"stage": "3"},
    dag=dag
)

start >> [print1_1, print2_1]
print1_1 >> print1_2
print2_1 >> print2_2
[print1_2, print2_2] >> print3