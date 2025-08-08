"""
# kapo_geschwindigkeitsmonitoring
This DAG updates the following datasets:

- [100112](https://data.bs.ch/explore/dataset/100112)
- [100115](https://data.bs.ch/explore/dataset/100115)
- [100097](https://data.bs.ch/explore/dataset/100097)
- [100200](https://data.bs.ch/explore/dataset/100200)
- [100358](https://data.bs.ch/explore/dataset/100358)
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
    "kapo_geschwindigkeitsmonitoring",
    description="Run the kapo_geschwindigkeitsmonitoring docker container",
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/kapo_geschwindigkeitsmonitoring:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        private_environment={
            **COMMON_ENV_VARS,
            "PG_CONNECTION": Variable.get("PG_CONNECTION"),
            "DETAIL_DATA_Q_BASE_PATH": Variable.get("DETAIL_DATA_Q_BASE_PATH"),
        },
        command="uv run -m etl",
        container_name="kapo_geschwindigkeitsmonitoring",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_geschwindigkeitsmonitoring/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/kapo_geschwindigkeitsmonitoring/change_tracking",
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
        mnt_tmp_dir=False,
        command="python3 -m rsync.sync_files kapo_geschwindigkeitsmonitoring.json",
        container_name="kapo_geschwindigkeitsmonitoring--rsync",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
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
