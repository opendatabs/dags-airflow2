"""
# ed_schulferien
This DAG updates the following dataset:

- [100397](https://data.bs.ch/explore/dataset/100397)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "renato.farruggio",
    "depend_on_past": False,
    "start_date": datetime(2024, 9, 18),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "ed_schulferien",
    default_args=default_args,
    description="Run the ed_schulferien docker container",
    schedule_interval="0 3 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/ed_schulferien:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100397": Variable.get("ODS_PUSH_URL_100397"),
        },
        container_name="ed_schulferien",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/ed_schulferien/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/ED-Schulferien",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/ed_schulferien/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
