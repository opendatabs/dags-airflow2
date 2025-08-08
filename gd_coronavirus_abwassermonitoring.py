"""
# gd_coronavirus_abwassermonitoring.py
This DAG updates the following datasets:

- [100167](https://data.bs.ch/explore/dataset/100167)
- [100187](https://data.bs.ch/explore/dataset/100187)
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
    "start_date": datetime(2024, 1, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "gd_coronavirus_abwassermonitoring",
    default_args=default_args,
    description="Run the gd_coronavirus_abwassermonitoring docker container",
    schedule="0 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/gd_coronavirus_abwassermonitoring:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="gd_coronavirus_abwassermonitoring--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/GD-Kantonslabor/Covid-19_Abwasser",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/gd_coronavirus_abwassermonitoring/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
