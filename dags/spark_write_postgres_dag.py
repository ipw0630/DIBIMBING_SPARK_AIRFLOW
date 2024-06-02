from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'dibimbing',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'spark_write_postgres_dag',
    default_args=default_args,
    description='DAG to trigger Spark job that writes data to PostgreSQL',
    schedule_interval='@daily',
)

# Define the Spark submit command
spark_submit_cmd = """
    /path/to/spark/bin/spark-submit \
    --master local[2] \
    /path/to/spark-example.py
"""

spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command=spark_submit_cmd,
    dag=dag,
)
