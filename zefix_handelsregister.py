"""
# zefix_handelsregister.py
This DAG updates the following datasets:

- [100330](https://data.bs.ch/explore/dataset/100330)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "orhan.saeedi",
    "description": "Run the zefix-handelsregister docker container",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 12),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "zefix_handelsregister",
    description="Run the zefix-handelsregister docker container",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/zefix_handelsregister:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl ",
        container_name="zefix_handelsregister",
        private_environment=COMMON_ENV_VARS,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/zefix_handelsregister/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/zefix_handelsregister/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
