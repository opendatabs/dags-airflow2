"""
# euroairport
This DAG updates the following datasets:

- [100078](https://data.bs.ch/explore/dataset/100078)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

# DAG configuration
DAG_ID = "euroairport"
FAILURE_THRESHOLD = 0  # Immediate failure with no skipping
EXECUTION_TIMEOUT = timedelta(minutes=10)
SCHEDULE = "*/15 5-8 * * *"

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
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
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_06": Variable.get("FTP_USER_06"),
            "FTP_PASS_06": Variable.get("FTP_PASS_06"),
        },
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
                source="/mnt/OGD-DataExch/EuroAirport",
                target="/code/data-processing/euroairport/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/euroairport/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    ods_publish = DockerOperator(
        task_id="ods-publish",
        image="ghcr.io/opendatabs/data-processing/ods_publish:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl_id 100078",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    upload >> ods_publish
