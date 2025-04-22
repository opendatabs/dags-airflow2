"""
# fgi_geodatenshop
This DAG updates the following datasets:

- [100395](https://data.bs.ch/explore/dataset/100395)
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
    "start_date": datetime(2024, 9, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    "fgi_geodatenshop",
    default_args=default_args,
    description="Run the fgi_geodatenshop docker container",
    schedule_interval="0 */2 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/fgi_geodatenshop:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_01": Variable.get("FTP_USER_01"),
            "FTP_PASS_01": Variable.get("FTP_PASS_01"),
        },
        container_name="fgi_geodatenshop",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/harvesters/FGI",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/fgi_geodatenshop/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    ods_harvest = DockerOperator(
        task_id="ods-harvest",
        image="ods_harvest:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m ods_harvest.etl gva-gpkg-ftp-csv",
        container_name="fgi_geodatenshop--ods_harvest",
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

upload >> ods_harvest
