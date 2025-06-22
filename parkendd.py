"""
# parkendd
This DAG updates the following datasets:

- [100014](https://data.bs.ch/explore/dataset/100014)
- [100044](https://data.bs.ch/explore/dataset/100044)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State
from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
from airflow.decorators import task
from airflow.providers.docker.hooks.docker import DockerHook
import json

DAG_ID = "parkendd"
FAILURE_VAR = f"{DAG_ID}_consecutive_failures"
FAILURE_THRESHOLD = 6  # fail on the 6th failure

default_args = {
    "owner": "jonas.bieri",
    "description": "Run the parkendd docker container",
    "depend_on_past": False,
    "start_date": datetime(2025, 6, 22),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

def execute_docker_with_failure_tracking(**context):
    """Wrapper function that executes Docker container and handles failures"""
    # Get failure count
    failure_count = int(Variable.get(FAILURE_VAR, default_var=0))
    
    try:
        # Use DockerOperator directly - it's the more reliable approach
        docker_operator = DockerOperator(
            task_id="docker_task",  # Internal task ID, not exposed
            image="ghcr.io/opendatabs/data-processing/parkendd:latest",
            api_version="auto",
            force_pull=True,
            auto_remove="force",
            command="uv run -m etl",
            private_environment=COMMON_ENV_VARS,
            container_name="parkendd",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            tty=True,
            mounts=[
                Mount(
                    source=f"{PATH_TO_CODE}/data-processing/parkendd/data",
                    target="/code/data",
                    type="bind",
                ),
                Mount(
                    source=f"{PATH_TO_CODE}/data-processing/parkendd/change_tracking",
                    target="/code/change_tracking",
                    type="bind",
                ),
            ],
        )
        
        # Execute the operator - it will raise an exception if the container exits non-zero
        result = docker_operator.execute(context)
        
        # If we get here, execution was successful
        Variable.set(FAILURE_VAR, 0)
        return result
        
    except Exception as e:
        if isinstance(e, AirflowSkipException):
            raise  # Re-raise skip exception
        
        # For other exceptions, count as a failure
        failure_count += 1
        Variable.set(FAILURE_VAR, failure_count)
        
        context['ti'].log.info(f"Docker execution failed: {str(e)}")
        
        if failure_count >= FAILURE_THRESHOLD:
            raise Exception(f"Upload failed {failure_count} times in a row: {str(e)}")
        else:
            raise AirflowSkipException(f"Upload failed {failure_count} times: {str(e)}")

with DAG(
    dag_id=DAG_ID,
    description="Run the parkendd docker container",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    execute_task = PythonOperator(
        task_id="run_docker",
        python_callable=execute_docker_with_failure_tracking,
        provide_context=True,
    )
