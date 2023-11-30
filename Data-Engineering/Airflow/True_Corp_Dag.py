from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}

dag = DAG(
    'True_Corp_hive_data_load',
    default_args=default_args,
    schedule_interval=None,
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

submit_job = SparkSubmitOperator(
    task_id='submit_job',
    application= '/scripts/hive_dataload.py',
    conn_id = 'spark_con1',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executor='1',
    driver_memory='2g',
    verobose=False
)

submit_job 
