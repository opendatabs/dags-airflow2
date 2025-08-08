"""
# mobilitaet_verkehrszaehldaten
This DAG updates the following datasets:

- [100006](https://data.bs.ch/explore/dataset/100006)
- [100013](https://data.bs.ch/explore/dataset/100013)
- [100356](https://data.bs.ch/explore/dataset/100356)
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
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "mobilitaet_verkehrszaehldaten",
    description="Run the mobilitaet_verkehrszaehldaten docker container",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/mobilitaet_verkehrszaehldaten:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_09": Variable.get("FTP_USER_09"),
            "FTP_PASS_09": Variable.get("FTP_PASS_09"),
        },
        container_name="mobilitaet_verkehrszaehldaten",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/mobilitaet_verkehrszaehldaten/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/MOB-StatA",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/mobilitaet_verkehrszaehldaten/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    ods_publish = DockerOperator(
        task_id="ods-publish",
        image="ghcr.io/opendatabs/data-processing/ods_publish:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl_id 100006,100013,100356",
        private_environment=COMMON_ENV_VARS,
        container_name="mobilitaet_verkehrszaehldaten--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    rsync = DockerOperator(
        task_id="rsync",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files mobilitaet_verkehrszaehldaten.json",
        container_name="mobilitaet_verkehrszaehldaten--rsync",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source="/data/dev/workspace", target="/code", type="bind"),
        ],
    )

    upload >> ods_publish
    upload >> rsync
