import time
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="spark_test",
    description='Hello World DAG',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False
)

spark_submit = SparkSubmitOperator(
	task_id='spark_test',
    conn_id='spark_conn',
    application='plugins/spark_1.py',
    executor_cores=2,
    executor_memory='471859200',
    name='spark_submit',
    dag=dag
)

spark_submit
