import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from operators.postgres_to_s3_operator import PostgresToS3Operator
from operators.s3_copy_object_operator import S3CopyObjectOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'source_bucket': os.environ['SOURCE_BUCKET']
}

custom_param_per_dag = {
    'sg': {
        'dest_bucket': os.environ['DEST_BUCKET_SG'],
        'schedule_interval': '0 17 * * *',  # 1 AM SGT 
    },
    'eu': {
        'dest_bucket': 'test-pyconapac-eu',
        'schedule_interval': '0 0 * * *',  # 1 AM CET
    },
    'us': {
        'dest_bucket': 'test-pyconapac-us',
        'schedule_interval': '0 9 * * *',  # 1 AM PST
    },
}

for region, v in custom_param_per_dag.items():
    dag = DAG(
        'shipment_{}'.format(region),
        default_args=default_args,
        schedule_interval=v['schedule_interval'],
        max_active_runs=1
    )

    t1 = PostgresToS3Operator(
        task_id='db_to_s3',
        sql="SELECT * FROM shipment WHERE region = '{{ params.region }}' AND ship_date = '{{ execution_date.strftime(\"%Y-%m-%d\") }}'",
        bucket=default_args['source_bucket'],
        object_key='{{ params.region }}/{{ execution_date.strftime("%Y%m%d%H%M%S") }}.csv',
        params={'region':region},
        dag=dag)

    t2 = S3CopyObjectOperator(
        task_id='s3_to_s3',
        source_object_key='{{ params.region }}/{{ execution_date.strftime("%Y%m%d%H%M%S") }}.csv',
        dest_object_key='{{ execution_date.strftime("%Y%m%d%H%M%S") }}.csv',
        source_bucket_name=default_args['source_bucket'],
        dest_bucket_name=v['dest_bucket'],
        params={'region': region},
        dag=dag)

    t1 >> t2

    globals()[dag] = dag
