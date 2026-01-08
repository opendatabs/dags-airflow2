"""
# staka_gutachten.py
This DAG helps populating the following datasets:

[100489](https://data.bs.ch/explore/dataset/100489)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "staka_gutachten"
FAILURE_THRESHOLD = 1
EXECUTION_TIMEOUT = timedelta(minutes=3)
SCHEDULE = "*/5 * * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 11, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command='''
            docker rm -f {DAG_ID}--upload 2>/dev/null || true
            ''',
    )

    # Main task to run the script
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/PD-Staka-Gutachten",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    # Set the task dependency
    cleanup_containers >> upload
