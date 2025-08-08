"""
# stata_pull_changes
This DAG pulls recent changes from the main/master branch of the following repositories:

- [data-processing](https://github.com/opendatabs/data-processing)
- [dags-airflow2](https://github.com/opendatabs/dags-airflow2)
- [startercode-opendatabs](https://github.com/opendatabs/startercode-generator-bs)
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
    "start_date": datetime(2024, 4, 4),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "stata_pull_changes",
    description="Run git pull on multiple repositories",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    git_pull = DockerOperator(
        task_id="git_pull",
        image="ghcr.io/opendatabs/data-processing/stata_pull_changes:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmo_dir=False,
        command="/bin/bash /code/data-processing/stata_pull_changes/pull_changes.sh ",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_pull_changes",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source=PATH_TO_CODE, target="/code", type="bind")],
    )
