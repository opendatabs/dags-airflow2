"""
# aue_schall
This DAG updates the following datasets:

- [100087](https://data.bs.ch/explore/dataset/100087)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

# DAG configuration
DAG_ID = "aue_schall"
FAILURE_THRESHOLD = 2  # Skip first 2 failures, fail on the 3rd failure
EXECUTION_TIMEOUT = timedelta(minutes=10)
SCHEDULE = "*/15 * * * *"

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 19),
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
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_04": Variable.get("FTP_USER_04"),
            "FTP_PASS_04": Variable.get("FTP_PASS_04"),
            "ODS_PUSH_URL_100087": Variable.get("ODS_PUSH_URL_100087"),
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
            )
        ],
    )
