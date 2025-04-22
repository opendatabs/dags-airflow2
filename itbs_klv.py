"""
# itbs_klv
This DAG updates the following datasets:

- [100324](https://data.bs.ch/explore/dataset/100324)
- [100325](https://data.bs.ch/explore/dataset/100325)
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
    "start_date": datetime(2024, 1, 17),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "itbs_klv",
    default_args=default_args,
    description="Run the itbs_klv docker container",
    schedule_interval="0 */2 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/itbs_klv:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "URL_LEISTUNGEN": Variable.get("URL_LEISTUNGEN"),
            "URL_GEBUEHREN": Variable.get("URL_GEBUEHREN"),
            "HOST_KLV": Variable.get("HOST_KLV"),
            "API_USER_KLV": Variable.get("API_USER_KLV"),
            "API_PASS_KLV": Variable.get("API_PASS_KLV"),
        },
        container_name="itbs_klv--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/itbs_klv/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/itbs_klv/data_orig",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/itbs_klv/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
