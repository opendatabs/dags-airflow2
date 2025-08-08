"""
# luftqualitaet_ch
This DAG updates the following datasets:

- [100048](https://data.bs.ch/explore/dataset/100048)
- [100049](https://data.bs.ch/explore/dataset/100049)
- [100050](https://data.bs.ch/explore/dataset/100050)
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
    "start_date": datetime(2024, 1, 29),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "luftqualitaet_ch",
    default_args=default_args,
    description="Run the luftqualitaet_ch docker container",
    schedule="15 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/luftqualitaet_ch:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_07": Variable.get("FTP_USER_07"),
            "FTP_PASS_07": Variable.get("FTP_PASS_07"),
            "ODS_PUSH_URLS_LUFTQUALITAET_CH": Variable.get(
                "ODS_PUSH_URLS_LUFTQUALITAET_CH"
            ),
        },
        container_name="luftqualitaet_ch",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/luftqualitaet_ch/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/luftqualitaet_ch/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
