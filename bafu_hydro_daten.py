"""
# bafu_hydro_daten
This DAG updates the following datasets:

- [100089](https://data.bs.ch/explore/dataset/100089)
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
    "start_date": datetime(2024, 1, 22),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "bafu_hydro_daten",
    description="Run the bafu_hydrodaten docker container",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/bafu_hydrodaten:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="uv run -m etl_https",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_01": Variable.get("HTTPS_URL_01"),
            "HTTPS_USER_01": Variable.get("HTTPS_USER_01"),
            "HTTPS_PASS_01": Variable.get("HTTPS_PASS_01"),
            "RHEIN_FILES": Variable.get("RHEIN_FILES"),
            "BIRS_FILES": Variable.get("BIRS_FILES"),
            "WIESE_FILES": Variable.get("WIESE_FILES"),
            "RHEIN_KLINGENTHAL_FILES": Variable.get("RHEIN_KLINGENTHAL_FILES"),
            "ODS_PUSH_URL_100089": Variable.get("ODS_PUSH_URL_100089"),
            "ODS_PUSH_URL_100236": Variable.get("ODS_PUSH_URL_100236"),
            "ODS_PUSH_URL_100235": Variable.get("ODS_PUSH_URL_100235"),
            "ODS_PUSH_URL_100243": Variable.get("ODS_PUSH_URL_100243"),
        },
        container_name="bafu_hydrodaten",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bafu_hydrodaten/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/bafu_hydrodaten/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
