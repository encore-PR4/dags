from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
        'preprocessing',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=3)
        },
    max_active_tasks=1,
    max_active_runs=1,
    schedule="0 9 * * *",
    start_date=datetime(2024, 10, 25),
    catchup=True,
    tags=['preprocessing','team4'],
) as dag:

    task_kafka_c = BashOperator(
        task_id="kafka_c",
        bash_command="""
        echo ds_nodash : {{ds_nodash}} ;
        $SPARK_HOME/bin/spark-submit $SPARK_HOME/py/spark_stream.py{{ ds_nodash }} """,
        )
    
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    
    task_start >> task_kafka_c >> task_end
