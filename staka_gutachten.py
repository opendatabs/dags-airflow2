"""
# staka_gutachten.py
This DAG helps populating the following datasets:

[100489](https://data.bs.ch/explore/dataset/100489)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 11, 19),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "staka_gutachten",
    description="Run the staka_gutachten.py docker container",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command='''
            docker rm -f staka_gutachten 2>/dev/null || true
            ''',
    )

    # Main task to run the script
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/staka_gutachten:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="staka_gutachten--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_gutachten/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/PD-Staka-Gutachten",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/staka_gutachten/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    # Set the task dependency
    cleanup_containers >> upload
