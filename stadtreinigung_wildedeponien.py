"""
# stadtreinigung_wildedeponien
This DAG updates the following datasets:

- [100070](https://data.bs.ch/explore/dataset/100070)
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
    "stadtreinigung_wildedeponien",
    description="Run the stadtreinigung_wildedeponien docker container",
    default_args=default_args,
    schedule="0 7,14 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/stadtreinigung_wildedeponien:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_TBA_WILDEDEPONIEN": Variable.get("HTTPS_URL_TBA_WILDEDEPONIEN"),
            "HTTPS_USER_TBA": Variable.get("HTTPS_USER_TBA"),
            "HTTPS_PASS_TBA": Variable.get("HTTPS_PASS_TBA"),
        },
        container_name="stadtreinigung_wildedeponien--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_wildedeponien/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_wildedeponien/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
