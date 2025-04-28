"""
# stata_befragungen
This DAG updates the datasets outlined [here](https://data.bs.ch/explore/?sort=modified&q=befragung+statistisches+amt).
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
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "stata_befragungen",
    description="Run the stata_befragungen docker container",
    default_args=default_args,
    schedule_interval="5,35 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/stata_befragungen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_befragungen--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_befragungen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Befragungen/55plus_Ablage_StatA",
                target="/code/data_orig/55plus",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Befragungen/55plus_OGD",
                target="/code/data/55plus",
                type="bind",
            ),
        ],
    )
