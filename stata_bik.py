"""
# stata_bik
This DAG updates the following datasets:

- [100003](https://data.bs.ch/explore/dataset/100003)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

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
    "stata_bik",
    description="Run the stata_bik docker container",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    ods_publish = DockerOperator(
        task_id="stata_bik",
        image="ghcr.io/opendatabs/data-processing/stata_bik:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_bik",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_bik/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/BIK",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_bik/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
