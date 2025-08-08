"""
# staka_kandidaturen.py
This DAG helps populating the following datasets:

[100316](https://data.bs.ch/explore/dataset/100316)
[100317](https://data.bs.ch/explore/dataset/100317)
[100333](https://data.bs.ch/explore/dataset/100333)
[100334](https://data.bs.ch/explore/dataset/100334)
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
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "staka_kandidaturen",
    description="Run the staka_kandidaturen.py docker container",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/staka_kandidaturen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="staka_kandidaturen--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_kandidaturen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Wahlen-Abstimmungen/Kandidaturen",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_kandidaturen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
