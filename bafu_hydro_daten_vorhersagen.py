"""
# bafu_hydro_daten
This DAG updates the following datasets:

- [100271](https://data.bs.ch/explore/dataset/100271)
- [100272](https://data.bs.ch/explore/dataset/100272)
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
    "start_date": datetime(2024, 1, 22),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "bafu_hydrodaten_vorhersagen",
    default_args=default_args,
    description="Run the bafu_hydrodaten_vorhersagen docker container",
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/bafu_hydrodaten_vorhersagen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "DICT_URL_BAFU_VORHERSAGEN": Variable.get("DICT_URL_BAFU_VORHERSAGEN"),
            "HTTPS_USER_01": Variable.get("HTTPS_USER_01"),
            "HTTPS_PASS_01": Variable.get("HTTPS_PASS_01"),
        },
        container_name="bafu_hydrodaten_vorhersagen",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bafu_hydrodaten_vorhersagen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bafu_hydrodaten_vorhersagen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
