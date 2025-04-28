"""
# zrd_gesetzessammlung.py
This DAG updates the following datasets:

- [100354](https://data.bs.ch/explore/dataset/100354)
- [100355](https://data.bs.ch/explore/dataset/100355)
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
    "start_date": datetime(2024, 3, 1),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "zrd_gesetzessammlung",
    description="Run zrd_gesetzessammlung docker container",
    default_args=default_args,
    schedule_interval="0 8/12 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/zrd_gesetzessammlung:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="zrd_gesetzessammlung",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/zrd_gesetzessammlung/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/zrd_gesetzessammlung/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
