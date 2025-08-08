"""
# stata_parzellen
This DAG updates the following datasets:

- [100202](https://data.bs.ch/explore/dataset/100202)
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
    "stata_parzellen",
    description="Run the stata_parzellen docker container",
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/stata_parzellen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_parzellen--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_parzellen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Parzellen",
                target="/code//data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_parzellen/data_orig",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
