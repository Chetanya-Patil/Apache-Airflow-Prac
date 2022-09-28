from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator


dag_id = "Databricks_Airflow_Integration"

default_args = {
    'owner': 'Chetanya',
    'databricks_conn_id': 'databricks_default'
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1)
)

# Spark Cluster Configuration
new_cluster = {
    'spark_version': '10.4.x-scala2.12',
    'node_type_id' : 'Standard_DS3_v2',
    'driver_node_type_id': 'Standard_DS3_v2',
    'num_workers': 1

}


# Spark Submit Operator
task1 = DatabricksSubmitRunOperator(
    task_id='ExecuteDatabricksOperator',
    spark_jar_task={'main_class_name': 'com.engineerestate.Example_01'},     # Class Name
    libraries=[{'jar': ''}],                      # Jar Path
    new_cluster=new_cluster,
    dag=dag
)

# BashOperator
task2 = BashOperator(
    task_id='DatabricksJobexecuted',
    bash_command='echo DatabricksAirflowIntegrationdone',
    trigger_rule='all_success',
    dag=dag
)


task1.set_downstream(task2)

