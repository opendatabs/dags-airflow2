"""
# mkb_sammlung_europa.py
This DAG updates the following datasets:

- [100148](https://data.bs.ch/explore/dataset/100148)

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "hester.pieters",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "mkb_sammlung_europa",
    default_args=default_args,
    description="Run the mkb_sammlung_europa docker container",
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/mkb_sammlung_europa:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="mkb_sammlung_europa--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/mkb_sammlung_europa/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/PD-Kultur-MKB",
                target="/code/data",
                type="bind",
            ),
        ],
    )
