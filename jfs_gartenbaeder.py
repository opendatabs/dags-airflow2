"""
# jfs_gartenbaeder
This DAG updates the following datasets:

- [100384](https://data.bs.ch/explore/dataset/100384)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "jfs_gartenbaeder"
FAILURE_THRESHOLD = 14
EXECUTION_TIMEOUT = timedelta(minutes=2)
SCHEDULE = "*/15 * * * *"

default_args = {
    "owner": "rstam.aloush",
    "depend_on_past": False,
    "start_date": datetime(2024, 8, 14),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        private_environment=COMMON_ENV_VARS,
        command="uv run -m etl",
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
