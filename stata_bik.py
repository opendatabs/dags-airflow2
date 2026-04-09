"""
# stata_bik
This DAG updates the following datasets:

- [100003](https://data.bs.ch/explore/dataset/100003)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

# DAG configuration
DAG_ID = "stata_bik"
FAILURE_THRESHOLD = 0  # Immediate failure with no skipping
EXECUTION_TIMEOUT = timedelta(minutes=10)
SCHEDULE = "*/15 * * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 3, 4),
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

    ods_publish = FailureTrackingDockerOperator(
        task_id="stata_bik",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
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
                source="/mnt/OGD-DataExch/StatA/BIK",
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
