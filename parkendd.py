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

DAG_ID = "parkendd"
FAILURE_VAR = f"{DAG_ID}_consecutive_failures"
FAILURE_THRESHOLD = 6  # fail on the 6th failure

default_args = {
    "owner": "jonas.bieri",
    "description": "Run the parkendd docker container",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

def handle_docker_result(**context):
    ti = context["ti"]
    previous_task_id = "run_docker"

    task_instance = context["dag"].get_task(previous_task_id)
    prev_ti = TaskInstance(task_instance=task_instance, execution_date=ti.execution_date)
    prev_ti.refresh_from_db()

    if prev_ti.state == State.SUCCESS:
        Variable.set(FAILURE_VAR, 0)
    else:
        count = int(Variable.get(FAILURE_VAR, default_var=0)) + 1
        Variable.set(FAILURE_VAR, count)
        if count >= 6:
            raise Exception(f"Upload failed {count} times in a row — raising failure.")
        else:
            raise AirflowSkipException(f"Upload failed {count} times, skipping without error.")

with DAG(
    dag_id=DAG_ID,
    description="Run the parkendd docker container",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    run_docker = DockerOperator(
        task_id="run_docker",
        image="ghcr.io/opendatabs/data-processing/parkendd:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="parkendd",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        do_xcom_push=True,  # important to capture container result
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

    check_result = PythonOperator(
        task_id="check_result",
        python_callable=handle_docker_result,
        provide_context=True,
    )

    run_docker >> check_result
