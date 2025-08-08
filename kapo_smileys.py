"""
# kapo_smileys
This DAG updates the following datasets:

- [100268](https://data.bs.ch/explore/dataset/100268)
- [100277](https://data.bs.ch/explore/dataset/100277)
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
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "kapo_smileys",
    default_args=default_args,
    description="Run kapo_smileys docker container",
    schedule="15 3 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/kapo_smileys:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="kapo_smileys--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_smileys/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/kapo-smileys",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_smileys/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    rsync = DockerOperator(
        task_id="rsync",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="python3 -m rsync.sync_files kapo_smileys.json",
        container_name="kapo_smileys--rsync",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
        ],
    )

    upload >> rsync
