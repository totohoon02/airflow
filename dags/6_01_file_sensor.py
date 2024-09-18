from pathlib import Path
from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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

    trigger_another_dag = TriggerDagRunOperator(
        task_id="run_task2",
        trigger_dag_id="06_02_01_another_dag",
    )

    sensor_task = wait_for_data("106659")
    start() >> sensor_task >> end() >> trigger_another_dag





task_builder()
