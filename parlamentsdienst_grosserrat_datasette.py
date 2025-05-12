"""
# parlamentsdienst_grosserrat_datasette.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 5, 8),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "parlamentsdienst_grosserrat_datasette",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat_datasette docker container",
    schedule_interval="22 13 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat_datasette:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="parlamentsdienst_grosserrat_datasette",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat/data/export",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat_datasette/data",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync = DockerOperator(
        task_id="rsync",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files parlamentsdienst_grosserra.json",
        container_name="parlamentsdienst_grosserrat--rsync",
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

    upload >> rsync
