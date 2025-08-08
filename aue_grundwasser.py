"""
# aue_grundwasser
This DAG updates the following datasets:

- [100164](https://data.bs.ch/explore/dataset/100164)
- [100179](https://data.bs.ch/explore/dataset/100179)
- [100180](https://data.bs.ch/explore/dataset/100180)
- [100181](https://data.bs.ch/explore/dataset/100181)
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
    "start_date": datetime(2024, 1, 17),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "aue_grundwasser",
    description="Run the aue_grundwasser docker container",
    default_args=default_args,
    schedule="25 5 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/aue_grundwasser:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_02": Variable.get("FTP_USER_02"),
            "FTP_PASS_02": Variable.get("FTP_PASS_02"),
        },
        container_name="aue_grundwasser--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_grundwasser/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_grundwasser/data_orig",
                target="/code/data_orig",
                type="bind",
            ),
        ],
    )
