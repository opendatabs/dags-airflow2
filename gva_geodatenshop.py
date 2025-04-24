from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "gva_geodatenshop",
    default_args=default_args,
    description="Run the gva-geodatenshop docker container",
    schedule_interval="0 5 * * *",
    catchup=False,
) as dag:
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/gva_geodatenshop:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_01": Variable.get("FTP_USER_01"),
            "FTP_PASS_01": Variable.get("FTP_PASS_01"),
        },
        container_name="gva_geodatenshop",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/gva_geodatenshop/data",
                target="/code/data",
                type="bind",
            ),
            Mount(source="/mnt/OGD-GVA", target="/code/data_orig", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/harvesters/GVA",
                target="/code/data_harvester",
                type="bind",
            ),
        ],
    )

    ods_harvest = DockerOperator(
        task_id="ods-harvest",
        image="ods_harvest:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m ods_harvest.etl gva-ftp-csv",
        container_name="gva-geodatenshop--ods_harvest",
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

    process_upload >> ods_harvest
