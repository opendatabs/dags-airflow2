"""
# dataspot
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "rstam.aloush",
    "description": "Run the dataspot docker container",
    "depend_on_past": False,
    "start_date": datetime(2024, 8, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dataspot",
    default_args=default_args,
    description="Run the dataspot docker container",
    schedule_interval="0 */2 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        private_environment=COMMON_ENV_VARS,
        command="uv run -m etl",
        container_name="dataspot",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/dataspot/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/dataspot/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/dataspot/.dataspot.env",
                target="/code/.dataspot.env",
                type="bind",
            ),
        ],
    )
