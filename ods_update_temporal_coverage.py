"""
# ods_update_temporal_coverage
This DAG automatically updates the temporal coverage of all datasets

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "renato.farruggio",
    "description": "Run the stata_ods/daily_jobs/update_temporal_coverage docker container",
    "depend_on_past": False,
    "start_date": datetime(2024, 10, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "ods_update_temporal_coverage",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    update = DockerOperator(
        task_id="update",
        image="ghcr.io/opendatabs/data-processing/ods_update_temporal_coverage:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_DOMAIN": Variable.get("ODS_DOMAIN"),
            "ODS_API_TYPE": Variable.get("ODS_API_TYPE"),
        },
        container_name="ods_update_temporal_coverage--update",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/ods_update_temporal_coverage",
                target="/code",
                type="bind",
            ),
        ],
    )
