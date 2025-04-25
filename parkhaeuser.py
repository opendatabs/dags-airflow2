"""
# parkhaeuser
This DAG updates the following datasets:

- [100088](https://data.bs.ch/explore/dataset/100088)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator

from common_variables import COMMON_ENV_VARS

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
    "parkhaeuser",
    default_args=default_args,
    description="Run the parkhaeuser docker container",
    schedule_interval="* * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/parkhaeuser:latest",
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100088": Variable.get("ODS_PUSH_URL_100088"),
        },
        container_name="parkhaeuser",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
