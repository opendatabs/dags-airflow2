"""
# aue_schall
This DAG updates the following datasets:

- [100087](https://data.bs.ch/explore/dataset/100087)
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
    "start_date": datetime(2024, 1, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "aue_schall",
    description="Run the aue_schall docker container",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/aue_schall:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_04": Variable.get("FTP_USER_04"),
            "FTP_PASS_04": Variable.get("FTP_PASS_04"),
            "ODS_PUSH_URL_100087": Variable.get("ODS_PUSH_URL_100087"),
        },
        container_name="aue_schall",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_schall/data",
                target="/code/data",
                type="bind",
            )
        ],
    )
