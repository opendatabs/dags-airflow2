"""
# staka_staatskalender.py
This DAG updates the following repositories:

[Starter Code on GitHub](https://github.com/opendatabs/startercode-opendatabs)
[Starter Code on Renku](https://renkulab.io/projects/opendatabs/startercode-opendatabs)

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
    "start_date": datetime(2024, 7, 3),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "startercode-generator-bs",
    description="Run the startercode-generator-bs docker container",
    default_args=default_args,
    schedule_interval="0 8/12 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    # Update task
    update = DockerOperator(
        task_id="update",
        image="startercode-generator-bs:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m updater",
        container_name="startercode-generator-bs",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/startercode-generator-bs",
                target="/code/startercode-generator-bs",
                type="bind",
            )
        ],
    )

    # GitHub update task
    update_github = DockerOperator(
        task_id="update_github",
        image="update_github:latest",
        api_version="auto",
        auto_remove="force",
        private_environment=COMMON_ENV_VARS,
        command="/bin/bash /code/startercode-generator-bs/update_github.sh ",
        container_name="update_github",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/startercode-generator-bs",
                target="/code/startercode-generator-bs",
                type="bind",
            )
        ],
    )

    # Renku update task
    update_renku = DockerOperator(
        task_id="update_renku",
        image="update_renku:latest",
        api_version="auto",
        auto_remove="force",
        private_environment=COMMON_ENV_VARS,
        command="/bin/bash /code/startercode-generator-bs/update_renku.sh ",
        container_name="update_renku",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/startercode-generator-bs",
                target="/code/startercode-generator-bs",
                type="bind",
            )
        ],
    )

    # Task dependencies
    update >> update_github
    update >> update_renku
