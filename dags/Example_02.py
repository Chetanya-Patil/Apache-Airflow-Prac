
# Scenario:-   Init  --> t1 --> t2,t3 ---> Complete
# Real Life Example: - Suppose we have generated a file(t1) which we have to move it to t2 (HDFS) and t3(MYSQL) after moving this workflow will be completed.

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id = "hello_world_dag_2",
    schedule_interval = "@daily",
    start_date = days_ago(1)
)

task1 = BashOperator(
    task_id = 't1',
    bash_command = 'echo hello',
    dag = dag
)
task2 = BashOperator(
    task_id = 't2',
    bash_command = 'echo t2',
    dag = dag
)

task3 = BashOperator(
    task_id = 't3',
    bash_command = 'echo t3',
    dag = dag
)

task1 >> [task2,task3]