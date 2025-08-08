"""
# stata_daily_upload
This DAG updates datasets referenced in https://github.com/opendatabs/data-processing/blob/master/stata_daily_upload/etl.py:
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

# DAG configuration
DAG_ID = "stata_daily_upload"
FAILURE_THRESHOLD = 16
EXECUTION_TIMEOUT = timedelta(minutes=2)
SCHEDULE = "*/15 * * * *"

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
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
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch",
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
