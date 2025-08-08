"""
# tba_wiese
This DAG updates the following datasets:

- [100269](https://data.bs.ch/explore/dataset/100269)
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
    "start_date": datetime(2024, 1, 31),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "tba_wiese",
    description="Run the tba_wiese docker container",
    default_args=default_args,
    schedule="30 * * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=50),
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/tba_wiese:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_TBA_WIESE": Variable.get("HTTPS_URL_TBA_WIESE"),
            "HTTPS_USER_TBA_WIESE": Variable.get("HTTPS_USER_TBA_WIESE"),
            "HTTPS_PASS_TBA_WIESE": Variable.get("HTTPS_PASS_TBA_WIESE"),
            "ODS_PUSH_URL_100269": Variable.get("ODS_PUSH_URL_100269"),
        },
        container_name="tba_wiese--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/tba_wiese/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/tba_wiese/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
