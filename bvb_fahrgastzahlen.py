"""
# bvb_fahrgastzahlen
This DAG updates the following datasets:

- [100075](https://data.bs.ch/explore/dataset/100075)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "rstam.aloush",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "bvb_fahrgastzahlen",
    default_args=default_args,
    description="Run the bvb_fahrgastzahlen docker container",
    schedule="*/30 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/bvb_fahrgastzahlen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="bvb_fahrgastzahlen",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bvb_fahrgastzahlen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bvb_fahrgastzahlen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/BVB/Fahrgastzahlen",
                target="/code/data_orig",
                type="bind",
            ),
        ],
    )
