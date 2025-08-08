"""
# smarte_strasse_ladestation
This DAG updates the following datasets:

- [100047](https://data.bs.ch/explore/dataset/100047)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator

from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "smarte_strasse_ladestation",
    description="Run the smarte_strasse_ladestation docker container",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/smarte_strasse_ladestation:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100047": Variable.get("ODS_PUSH_URL_100047"),
            "HTTPS_URL_LADESTATIONEN": Variable.get("HTTPS_URL_LADESTATIONEN"),
            "HTTPS_URL_AUTH_LADESTATIONEN": Variable.get(
                "HTTPS_URL_AUTH_LADESTATIONEN"
            ),
            "HTTPS_USER_LADESTATIONEN": Variable.get("HTTPS_USER_LADESTATIONEN"),
            "HTTPS_PASS_LADESTATIONEN": Variable.get("HTTPS_PASS_LADESTATIONEN"),
            "API_KEY_LADESTATIONEN": Variable.get("API_KEY_LADESTATIONEN"),
        },
        container_name="smarte_strasse_ladestation",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
