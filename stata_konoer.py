"""
# stata_konoer.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 11, 28),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}
with DAG(
    "stata_konoer",
    description="Run the stata_konoer docker container",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=False,
) as dag:
    transform = DockerOperator(
        task_id="transform",
        image="stata_konoer:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="Rscript /code/data-processing/stata_konoer/etl.R",
        container_name="stata_konoer--transform",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/data/dev/workspace/data-processing",
                target="/code/data-processing",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/KoNöR",
                target="/code/data-processing/stata_konoer/data",
                type="bind",
            ),
        ],
    )

    rsync_test = DockerOperator(
        task_id="rsync1",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files stata_konoer_test.json",
        container_name="stata_konoer--rsync_test",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=Variable.get("PATH_TO_CODE"), target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/KoNöR",
                target="/code/data-processing/stata_konoer/data",
                type="bind",
            ),
        ],
    )

    rsync_prod = DockerOperator(
        task_id="rsync2",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files stata_konoer_prod.json",
        container_name="stata_konoer--rsync_prod",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=Variable.get("PATH_TO_CODE"), target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/KoNöR",
                target="/code/data-processing/stata_konoer/data",
                type="bind",
            ),
        ],
    )

    transform >> rsync_test
    transform >> rsync_prod
