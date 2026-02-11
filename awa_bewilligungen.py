"""
# awa_bewilligungen
This DAG updates the following datasets:

- [100424](https://data.bs.ch/explore/dataset/100424)
- [100425](https://data.bs.ch/explore/dataset/100425)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

DAG_ID = "awa_bewilligungen"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2026, 2, 11),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule="16 16 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/AWA – Arbeitsinspektorat",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
