"""
# parkhaeuser
This DAG updates the following datasets:

- [100088](https://data.bs.ch/explore/dataset/100088)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

from common_variables import COMMON_ENV_VARS

# DAG configuration
DAG_ID = "parkhaeuser"
FAILURE_THRESHOLD = 2
EXECUTION_TIMEOUT = timedelta(seconds=50)
SCHEDULE = "* * * * *"

default_args = {
    "owner": "renato.farruggio",
    "depend_on_past": False,
    "start_date": datetime(2024, 10, 12),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = FailureTrackingDockerOperator(
        task_id="process-upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100088": Variable.get("ODS_PUSH_URL_100088"),
        },
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
    )
