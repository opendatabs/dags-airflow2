"""
# dcc_verzeichnis_personendaten
This DAG updates the following dataset:

- [100520](https://data.bs.ch/explore/dataset/100520)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

DAG_ID = "dcc_verzeichnis_personendaten"

default_args = {
    "owner": "renato.farruggio",
    "depend_on_past": False,
    "start_date": datetime(2026, 6, 5),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
    schedule="0 4 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "DATASPOT_EXPOSED_CLIENT_ID": Variable.get("DATASPOT_EXPOSED_CLIENT_ID"),
            "DATASPOT_TENANT_ID": Variable.get("DATASPOT_TENANT_ID"),
            "DATASPOT_CLIENT_ID": Variable.get("DATASPOT_CLIENT_ID"),
            "DATASPOT_CLIENT_SECRET": Variable.get("DATASPOT_CLIENT_SECRET"),
            "DATASPOT_SERVICE_USER_ACCESS_KEY": Variable.get("DATASPOT_SERVICE_USER_ACCESS_KEY"),
        },
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
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
