"""
# staka_regierungsratsbeschluesse.py
This DAG updates the following datasets:

- [100427](https://data.bs.ch/explore/dataset/100427)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 2, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "staka_regierungsratsbeschluesse",
    description="Run the staka_regierungsratsbeschluesse docker container",
    default_args=default_args,
    schedule="*/5 * * * 2,3",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/staka_regierungsratsbeschluesse:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="staka_regierungsratsbeschluesse",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_regierungsratsbeschluesse/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_regierungsratsbeschluesse/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
