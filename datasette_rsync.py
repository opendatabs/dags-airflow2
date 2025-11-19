"""
# datasette_rsync.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import PATH_TO_CODE, PATH_TO_DATASETTE_FILES

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 11, 14),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}
with DAG(
    "datasette_rsync",
    description="Run the datasette_rsync docker container",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
) as dag:
    rsync = DockerOperator(
        task_id="rsync",
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files datasette.json",
        container_name="datasette--rsync",
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
            Mount(
                source=PATH_TO_DATASETTE_FILES,
                target="/code/data",
                type="bind",
            ),
        ],
    )
