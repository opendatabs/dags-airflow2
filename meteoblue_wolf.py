"""
# meteoblue_wolf
This DAG updates the following datasets:

- [100009](https://data.bs.ch/explore/dataset/100009)
- [100082](https://data.bs.ch/explore/dataset/100082)
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
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "meteoblue_wolf",
    default_args=default_args,
    description="Run the meteoblue_wolf docker container",
    schedule="10 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/meteoblue_wolf:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "PUBLIC_KEY_FIELDCLIMATE": Variable.get("PUBLIC_KEY_FIELDCLIMATE"),
            "PRIVATE_KEY_FIELDCLIMATE": Variable.get("PRIVATE_KEY_FIELDCLIMATE"),
            "FTP_USER_08": Variable.get("FTP_USER_08"),
            "FTP_PASS_08": Variable.get("FTP_PASS_08"),
        },
        container_name="meteoblue_wolf",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/meteoblue_wolf/data",
                target="/code/data",
                type="bind",
            )
        ],
    )

    ods_publish = DockerOperator(
        task_id="ods-publish",
        image="ghcr.io/opendatabs/data-processing/ods_publish:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl_id 100009,100082",
        private_environment=COMMON_ENV_VARS,
        container_name="meteoblue_wolf--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    process_upload >> ods_publish
