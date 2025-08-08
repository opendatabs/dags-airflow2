"""
# tba_abfuhrtermine
This DAG updates the following datasets:

- [100096](https://data.bs.ch/explore/dataset/100096)
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
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "tba_abfuhrtermine",
    description="Run the tba_abfuhrtermine docker container",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/tba_abfuhrtermine:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="tba_abfuhrtermine",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/tba_abfuhrtermine/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-GVA",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/tba_abfuhrtermine/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
