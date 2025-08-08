"""
# staka_briefliche_stimmabgaben.py
This DAG helps populating the following datasets:

- [100223](https://data.bs.ch/explore/dataset/100223)
- [100224](https://data.bs.ch/explore/dataset/100224) (depubliziert)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "hester.pieters",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "staka_briefliche_stimmabgaben",
    description="Run the staka_briefliche_stimmabgaben docker container",
    default_args=default_args,
    schedule="30 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/staka_briefliche_stimmabgaben:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100223": Variable.get("ODS_PUSH_URL_100223"),
        },
        container_name="staka_briefliche_stimmabgaben--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/staka-abstimmungen",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_briefliche_stimmabgaben/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
