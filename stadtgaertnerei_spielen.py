"""
# tba_baustellen
This DAG updates the following datasets:

- [100462](https://data.bs.ch/explore/dataset/100462)
- [100463](https://data.bs.ch/explore/dataset/100463)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 5, 16),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "stadtgaertnerei_spielen",
    description="Run the stadtgaertnerei_spielen docker container",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/stadtgaertnerei_spielen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_TBA_SPIELPLAETZE": Variable.get("HTTPS_URL_TBA_SPIELPLAETZE"),
            "HTTPS_URL_TBA_SPIELGERAETE": Variable.get("HTTPS_URL_TBA_SPIELGERAETE"),
            "HTTPS_USER_TBA": Variable.get("HTTPS_USER_TBA"),
            "HTTPS_PASS_TBA": Variable.get("HTTPS_PASS_TBA"),
        },
        container_name="stadtgaertnerei_spielen",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtgaertnerei_spielen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtgaertnerei_spielen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
