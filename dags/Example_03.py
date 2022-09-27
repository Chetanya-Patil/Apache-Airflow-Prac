
#Scenario -
# if T1 is success:
#     Execute T2    ---When all of its parent success - (all_success)
# Else:
#     Execute T3    ---When all of its parent fail that time we have to execute TR - (all_failed)

# Solution: For this kind of things we have Trigger Rule.

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id = "hello_world_dag_3",
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
    dag = dag,
    trigger_rule = 'all_failed'
)

task1 >> [task2,task3]