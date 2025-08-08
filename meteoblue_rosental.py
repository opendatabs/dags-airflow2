"""
# bafu_hydro_daten
This DAG updates the following dataset:

- [100294](https://data.bs.ch/explore/dataset/100294)
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
    "start_date": datetime(2024, 1, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "meteoblue_rosental",
    default_args=default_args,
    description="Run the meteoblue_rosental docker container",
    schedule="45 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/meteoblue_rosental:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        private_environment={
            **COMMON_ENV_VARS,
            "API_KEY_METEOBLUE": Variable.get("API_KEY_METEOBLUE"),
            "FTP_USER_07": Variable.get("FTP_USER_07"),
            "FTP_PASS_07": Variable.get("FTP_PASS_07"),
            "ODS_PUSH_URL_100294": Variable.get("ODS_PUSH_URL_100294"),
        },
        command="uv run -m etl",
        container_name="meteoblue_rosental",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/meteoblue_rosental/data",
                target="/code/data",
                type="bind",
            )
        ],
    )
