from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import os, sys
sys.path.append('/app')  # Adjust if needed

from main import run_pipeline

#could be run as dag , take source_type for loading from external place or per time delta vs type
#or create tasks with different source types
# possible options for extend solution for use airflow as orchestrator
# Create operator that run deffer task, use Triggerer - add file to the observing folder for example
# or use sql or stream for db case
# polling with timedelta interval for the kafka
# in the case of api use timedelta loop

with DAG(
    dag_id='duality_pipeline_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    source_types = Variable.get('duality_pipeline')
    # could use triggerer for start task
    for source_type in source_types:

        task = PythonOperator(
            task_id='run_duality_pipeline',
            python_callable=run_pipeline,
            op_kwargs={'source_type': source_type}
        )