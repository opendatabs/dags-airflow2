"""
# stata_tourismusdashboard.py
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from pytz import timezone

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")
http_proxy = Variable.get("http_proxy")
PATH_TO_CODE = Variable.get("PATH_TO_CODE")
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


def check_embargo_timestamp(file_path: str, **kwargs):
    tz = timezone("Europe/Zurich")
    now = datetime.now(tz)

    if not os.path.exists(file_path):
        raise AirflowSkipException(f"Embargo file {file_path} does not exist.")

    with open(file_path, "r") as f:
        timestamp_str = f.read().strip()

    try:
        embargo_time = datetime.fromisoformat(timestamp_str).astimezone(tz)
    except ValueError as e:
        raise AirflowSkipException(f"Invalid timestamp format in {file_path}: {e}")

    if now - embargo_time > timedelta(hours=24):
        raise AirflowSkipException(
            f"Embargo timestamp {embargo_time.isoformat()} in {file_path} is older than 24 hours."
        )


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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
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
        command="python3 -m rsync.sync_files stata_tourismus_100413_prod.json",
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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
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
        command="python3 -m rsync.sync_files stata_tourismus_100413_prod.json",
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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    embargo_100413 = DockerOperator(
        task_id="embargo_100413",
        image="python:3.12-slim",
        command="python3 /code/check_embargo.py /code/data/100413_tourismus-daily_embargo.txt",
        mounts=[
            Mount(source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard", target="/code", type="bind"),
            Mount(source="/mnt/OGD-DataExch/StatA/Tourismus", target="/code/data", type="bind"),
        ],
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    rsync_100413_public = DockerOperator(
        task_id="rsync_100413_public",
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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    embargo_100414 = DockerOperator(
        task_id="embargo_100414",
        image="python:3.12-slim",
        command="python3 /code/check_embargo.py /code/data/100414_tourismus-daily_embargo.txt",
        mounts=[
            Mount(source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard", target="/code", type="bind"),
            Mount(source="/mnt/OGD-DataExch/StatA/Tourismus", target="/code/data", type="bind"),
        ],
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    rsync_100414_public = DockerOperator(
        task_id="rsync_100414_public",
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
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    (
        upload
        >> rsync_100413_test
        >> rsync_100414_test
        >> rsync_100413_prod
        >> rsync_100414_prod
        >> embargo_100413
        >> rsync_100413_public
        >> embargo_100414
        >> rsync_100414_public
    )
