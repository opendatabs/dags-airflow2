"""
# staka_kantonsblatt.py
This DAG updates the following datasets:

- [100352](https://data.bs.ch/explore/dataset/100352)
- [100366](https://data.bs.ch/explore/dataset/100366)
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "staka_kantonsblatt",
    default_args=default_args,
    description="Run the staka_kantonsblatt and staka_baupublikationen docker container",
    schedule_interval="30 0 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_kantonsblatt = DockerOperator(
        task_id="upload_kantonsblatt",
        image="ghcr.io/opendatabs/data-processing/staka_kantonsblatt:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "EXCEL_PATH_BAUPUBLIKATIONEN_FOR_MAIL": Variable.get(
                "EXCEL_PATH_BAUPUBLIKATIONEN_FOR_MAIL"
            ),
        },
        container_name="staka_kantonsblatt",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Staatskanzlei/Kantonsblatt",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_kantonsblatt/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    # https://stackoverflow.com/questions/55002234/apache-airflow-delay-a-task-for-some-period-of-time
    # Wait for 30 minutes for data to be updated
    delay_python_task: PythonOperator = PythonOperator(
        task_id="delay_python_task", python_callable=lambda: time.sleep(1800)
    )

    upload_baupublikation = DockerOperator(
        task_id="upload_baupublikation",
        image="ghcr.io/opendatabs/data-processing/staka_baupublikationen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "API_KEY_MAPBS": Variable.get("API_KEY_MAPBS")
        }
        container_name="staka_baupublikationen",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_baupublikationen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_baupublikationen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    upload_kantonsblatt >> delay_python_task >> upload_baupublikation
