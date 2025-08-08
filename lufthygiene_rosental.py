"""
# bafu_hydro_daten
This DAG updates the following dataset:

- [100275](https://data.bs.ch/explore/dataset/100275)
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
    "start_date": datetime(2024, 1, 29),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "lufthygiene_rosental",
    default_args=default_args,
    description="Run the lufthygiene_rosental docker container",
    schedule="*/30 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/lufthygiene_rosental:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_07": Variable.get("FTP_USER_07"),
            "FTP_PASS_07": Variable.get("FTP_PASS_07"),
            "ODS_PUSH_URL_100275": Variable.get("ODS_PUSH_URL_100275"),
        },
        container_name="lufthygiene_rosental",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/lufthygiene_rosental/data",
                target="/code/data",
                type="bind",
            )
        ],
    )
