"""
# stata_superblock_allmend.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "stata_superblock_allmend"
FAILURE_THRESHOLD = 0
SCHEDULE = "20 4 * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2026, 1, 14),
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

    # Main task to run the script
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        failure_threshold=FAILURE_THRESHOLD,
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
                source="/mnt/OGD-DataExch/StatA/KoNöR/Allmend",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/KoNöR/Allmend",
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
