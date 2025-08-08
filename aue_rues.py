"""
# aue_rues
This DAG updates the following datasets:

- [100046](https://data.bs.ch/explore/dataset/100046)
- [100323](https://data.bs.ch/explore/dataset/100323)
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
    "start_date": datetime(2024, 1, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "aue_rues",
    description="Run the aue_rues docker container",
    default_args=default_args,
    schedule="*/10 * * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=8),
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/aue_rues:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_03": Variable.get("FTP_USER_03"),
            "FTP_PASS_03": Variable.get("FTP_PASS_03"),
            "ODS_PUSH_URL_100046": Variable.get("ODS_PUSH_URL_100046"),
            "ODS_PUSH_URL_100323": Variable.get("ODS_PUSH_URL_100323"),
        },
        container_name="aue_rues--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_rues/data_orig",
                target="/code/data_orig",
                type="bind",
            )
        ],
    )
