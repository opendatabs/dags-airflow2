"""
# kapo_eventverkehr_stjakob.py
This DAG updates the following datasets:

- [100419](https://data.bs.ch/explore/dataset/100419)
- [100429](https://data.bs.ch/explore/dataset/100429)
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
    "start_date": datetime(2025, 1, 31),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "kapo_eventverkehr_stjakob",
    default_args=default_args,
    description="Run the kapo_eventverkehr_stjakob docker container",
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/kapo_eventverkehr_stjakob:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="kapo_eventverkehr_stjakob",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_eventverkehr_stjakob/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/KaPo-Eventverkehr-St.Jakob",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_eventverkehr_stjakob/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
