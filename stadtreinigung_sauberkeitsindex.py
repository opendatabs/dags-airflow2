"""
# stadtreinigung_sauberkeitsindex
This DAG updates the following datasets:

- [100298](https://data.bs.ch/explore/dataset/100298)
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
    "start_date": datetime(2024, 3, 13),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "stadtreinigung_sauberkeitsindex",
    description="Run the stadtreinigung_sauberkeitsindex",
    default_args=default_args,
    schedule_interval="0 7,14 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/stadtreinigung_sauberkeitsindex:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_TBA_SAUBERKEITSINDEX": Variable.get(
                "HTTPS_URL_TBA_SAUBERKEITSINDEX"
            ),
            "HTTPS_USER_TBA": Variable.get("HTTPS_USER_TBA"),
            "HTTPS_PASS_TBA": Variable.get("HTTPS_PASS_TBA"),
        },
        container_name="stadtreinigung_sauberkeitsindex",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_sauberkeitsindex/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_sauberkeitsindex/data_agg",
                target="/code/data_agg",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_sauberkeitsindex/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
