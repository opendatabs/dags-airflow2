"""
# bafu_hydro_daten
This DAG updates the following datasets:

- [100271](https://data.bs.ch/explore/dataset/100271)
- [100272](https://data.bs.ch/explore/dataset/100272)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "bafu_hydrodaten_vorhersagen"
FAILURE_THRESHOLD = 5
EXECUTION_TIMEOUT = timedelta(minutes=2)
SCHEDULE = "0 * * * *"

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 22),
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
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl",
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        private_environment={
            **COMMON_ENV_VARS,
            "DICT_URL_BAFU_VORHERSAGEN": Variable.get("DICT_URL_BAFU_VORHERSAGEN"),
            "HTTPS_USER_01": Variable.get("HTTPS_USER_01"),
            "HTTPS_PASS_01": Variable.get("HTTPS_PASS_01"),
        },
        container_name=DAG_ID,
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
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
