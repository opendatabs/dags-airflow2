"""
# unibas_semesterdaten
This DAG updates the following datasets:

- [100469](https://data.bs.ch/explore/dataset/100469)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "rstam.aloush",
    "depend_on_past": False,
    "start_date": datetime(2025, 9, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "unibas_semesterdaten",
    default_args=default_args,
    description="Run the unibas_semesterdaten docker container",
    schedule="0 0 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    extract_data = DockerOperator(
        task_id="extract_data",
        image="ghcr.io/opendatabs/data-processing/unibas_semesterdaten:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="unibas_semesterdaten",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/unibas_semesterdaten/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/unibas_semesterdaten/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    create_ics = DockerOperator(
        task_id="create_ics",
        image="ghcr.io/opendatabs/data-processing/unibas_semesterdaten:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m semesterdaten_ics",
        private_environment=COMMON_ENV_VARS,
        container_name="unibas_semesterdaten",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/unibas_semesterdaten/data",
                target="/code/data",
                type="bind",
            ),
        ],
    )
    extract_data >> create_ics