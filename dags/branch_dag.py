from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id="04_02_branch_dag",
)


def _print(stage):
    print(stage)


start = EmptyOperator(
    task_id="dummy",
    dag=dag,
    trigger_rule="none_failed"
)

print_1_old = PythonOperator(
    task_id="print_1_old",
    python_callable=_print,
    op_kwargs={"stage": "old_1"},
    dag=dag,
    trigger_rule="none_failed"
)

print_2_old = PythonOperator(
    task_id="print_2_old",
    python_callable=_print,
    op_kwargs={"stage": "old_2"},
    dag=dag,
    trigger_rule="none_failed"
)

print_1_new = PythonOperator(
    task_id="print_1_new",
    python_callable=_print,
    op_kwargs={"stage": "new_1"},
    dag=dag,
    trigger_rule="none_failed"
)

print_2_new = PythonOperator(
    task_id="print_2_new",
    python_callable=_print,
    op_kwargs={"stage": "new_2"},
    dag=dag,
    trigger_rule="none_failed"
)

print_3 = PythonOperator(
    task_id="print3",
    python_callable=_print,
    op_kwargs={"stage": "3"},
    dag=dag,
    trigger_rule="none_failed"
)

is_use_new = False


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

print_2_branch = BranchPythonOperator(
    task_id="pick_branch_2",
    python_callable=_pick_branch,
    op_kwargs={"stage": "2"},
    dag=dag
)

start >> print_1_branch >> [print_1_old, print_1_new] >> print_3
start >> print_2_branch >> [print_2_old, print_2_new] >> print_3
