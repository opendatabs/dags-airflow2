"""
# stata_tourismusdashboard.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")
http_proxy = Variable.get("http_proxy")
DB_CONNECTION_STRING_TOURISMUS = Variable.get("DB_CONNECTION_STRING_TOURISMUS")

default_args = {
    "owner": "orhan.saeedi",
    "description": "Run the stata_tourismusdashboard docker container",
    "depend_on_past": False,
    "start_date": datetime(2025, 4, 11),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}
with DAG(
    "stata_tourismusdashboard",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    catchup=False,
) as dag:
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/tourismusdashboard:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="Rscript /code/app_write_OGD.R",
        private_environment={
            "https_proxy": https_proxy,
            "http_proxy": http_proxy,
            "DB_CONNECTION_STRING_TOURISMUS": DB_CONNECTION_STRING_TOURISMUS,
        },
        container_name="stata_tourismusdashboard--transform",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            )
        ],
    )

    rsync_100413_test = DockerOperator(
        task_id="rsync_100413_test",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_100413_test.json",
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
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_100414_test = DockerOperator(
        task_id="rsync_100414_test",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_100413_test.json",
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
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_100413_prod = DockerOperator(
        task_id="rsync_100413_prod",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_100413_test.json",
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
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_100414_prod = DockerOperator(
        task_id="rsync_100414_prod",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_100413_test.json",
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
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    upload >> rsync_100413_test >> rsync_100414_test >> rsync_100413_prod >> rsync_100414_prod
