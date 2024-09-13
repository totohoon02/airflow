from airflow.decorators import task, dag
from datetime import datetime


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
