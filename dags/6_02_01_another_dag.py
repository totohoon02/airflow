from airflow.decorators import dag, task


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
