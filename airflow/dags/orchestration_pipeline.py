from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Maria',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'reddit_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    description="Pipeline Kafka -> Spark -> Cassandra/MongoDB"
) as dag:

    run_kafka = BashOperator(
    task_id='run_kafka',
    bash_command='/usr/local/bin/python /opt/airflow/main/data_ingestion/data_ingestion.py'
    )

    run_spark = BashOperator(
        task_id='run_spark',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/run.py'
    )

