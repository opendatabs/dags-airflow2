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
    "euroairport",
    default_args=default_args,
    description="Run the euroairport docker container",
    schedule_interval="*/15 5-8 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/euroairport:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_06": Variable.get("FTP_USER_06"),
            "FTP_PASS_06": Variable.get("FTP_PASS_06"),
        },
        container_name="euroairport",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/euroairport/data",
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
        image="ods_publish:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m ods_publish.etl_id 100078",
        container_name="euroairport--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing",
                target="/code/data-processing",
                type="bind",
            )
        ],
    )

    upload >> ods_publish
