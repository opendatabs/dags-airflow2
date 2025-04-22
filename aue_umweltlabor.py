"""
# aue-umweltlabor
This DAG updates the following datasets:

- [100066](https://data.bs.ch/explore/dataset/100066)
- [100067](https://data.bs.ch/explore/dataset/100067)
- [100068](https://data.bs.ch/explore/dataset/100068)
- [100069](https://data.bs.ch/explore/dataset/100069)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "jonas.bieri",
    "description": "Run the aue-umweltlabor docker container",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    "aue_umweltlabor",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/aue_umweltlabor:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_05": Variable.get("FTP_USER_05"),
            "FTP_PASS_05": Variable.get("FTP_PASS_05"),
            "ODS_PUSH_URL_100069": Variable.get("ODS_PUSH_URL_100069"),
        },
        container_name="aue_umweltlabor",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_umweltlabor/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/Umweltlabor",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_umweltlabor/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    ods_publish = DockerOperator(
        task_id="ods-publish",
        image="ods_publish:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m ods_publish.etl_id 100066,100067,100068",
        container_name="aue_umweltlabor--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing",
                target="/code/data-processing",
                type="bind",
            )
        ],
    )

    process_upload >> ods_publish
