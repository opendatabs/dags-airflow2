"""
# stata_hunde
This DAG updates the following datasets:

- [100444](https://data.bs.ch/explore/dataset/100444)
- [100445](https://data.bs.ch/explore/dataset/100445)
- [100446](https://data.bs.ch/explore/dataset/100446)
- [100447](https://data.bs.ch/explore/dataset/100447)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 5, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "stata_hunde",
    description="Run the stata_hunde docker container",
    default_args=default_args,
    schedule="49 14 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/stata_hunde:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_hunde--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Hunde",
                target="/code/data",
                type="bind",
            ),
        ],
    )
