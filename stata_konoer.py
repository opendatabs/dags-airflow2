"""
# stata_konoer.py
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
    "start_date": datetime(2024, 11, 28),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}
with DAG(
    "stata_konoer",
    description="Run the stata_konoer docker container",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=False,
) as dag:
    transform = DockerOperator(
        task_id="transform",
        image="ghcr.io/opendatabs/stata_konoer:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="Rscript etl.R",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_konoer--transform",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing//kapo_ordnungsbussen/data",
                target="/code/data_orig/ordnungsbussen",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stadtreinigung_wildedeponien/data",
                target="/code/data_orig/wildedeponien",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/tba_sprayereien/data",
                target="/code/data_orig/sprayereien",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/stata_konoer/data",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_test = DockerOperator(
        task_id="rsync1",
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files stata_konoer_test.json",
        container_name="stata_konoer--rsync_test",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
        ],
    )

    rsync_prod = DockerOperator(
        task_id="rsync2",
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files stata_konoer_prod.json",
        container_name="stata_konoer--rsync_prod",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
        ],
    )

    transform >> rsync_test
    transform >> rsync_prod
