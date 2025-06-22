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
        # Configure the Docker hook and parameters similar to DockerOperator
        hook = DockerHook(
            docker_conn_id='docker_default',
            base_url='unix://var/run/docker.sock',
        )
        
        # Configure container options
        container = hook.client.containers.run(
            image="ghcr.io/opendatabs/data-processing/parkendd:latest",
            command="uv run -m etl",
            environment=COMMON_ENV_VARS,
            name="parkendd",
            network_mode="bridge",
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
            auto_remove=True,
            detach=True
        )
        
        # Stream logs and wait for completion
        for line in container.logs(stream=True):
            context['ti'].log.info(line.decode().strip())
        
        # Get exit code
        result = container.wait()
        exit_code = result['StatusCode']
        
        if exit_code == 0:
            # Success - reset counter
            Variable.set(FAILURE_VAR, 0)
            return True
        else:
            # Failed - increment counter
            failure_count += 1
            Variable.set(FAILURE_VAR, failure_count)
            
            # Check threshold
            if failure_count >= FAILURE_THRESHOLD:
                raise Exception(f"Upload failed {failure_count} times in a row — raising failure.")
            else:
                # Skip without error
                context['ti'].log.info(f"Upload failed {failure_count} times, skipping without error.")
                # This is the key part - we'll mark this as skipped instead of failed
                raise AirflowSkipException(f"Upload failed {failure_count} times, skipping without error.")
    
    except Exception as e:
        if isinstance(e, AirflowSkipException):
            raise  # Re-raise skip exception
        
        # For other exceptions, check if we should count it as a failure
        failure_count += 1
        Variable.set(FAILURE_VAR, failure_count)
        
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
