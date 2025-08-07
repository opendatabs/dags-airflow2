"""
# staka_abstimmungen
This DAG updates the test and live version of the 2 datasets that cover the latest polls:

- [100141](https://data.bs.ch/explore/dataset/100141)
- [100142](https://data.bs.ch/explore/dataset/100142)
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
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "staka_abstimmungen",
    description="Run the staka_abstimmungen docker container",
    default_args=default_args,
    schedule="*/2 9-19 * * 7",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/staka_abstimmungen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100343": Variable.get("ODS_PUSH_URL_100343"),
            "ODS_PUSH_URL_100344": Variable.get("ODS_PUSH_URL_100344"),
            "ODS_PUSH_URL_100345": Variable.get("ODS_PUSH_URL_100345"),
            "ODS_PUSH_URL_100346": Variable.get("ODS_PUSH_URL_100346"),
        },
        container_name="staka_abstimmungen",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Wahlen-Abstimmungen",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_abstimmungen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
