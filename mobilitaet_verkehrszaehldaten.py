"""
# mobilitaet_verkehrszaehldaten
This DAG updates the following datasets:

- [100006](https://data.bs.ch/explore/dataset/100006)
- [100013](https://data.bs.ch/explore/dataset/100013)
- [100356](https://data.bs.ch/explore/dataset/100356)
- [Datasette MIV](https://datatools.bs.ch/MIV)
- [Datasette Velo Fuss](https://datatools.bs.ch/Velo_Fuss)
- [Datasette MIV Geschwindigkeitsklassen](https://datatools.bs.ch/MIV_Geschwindigkeitsklassen)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "mobilitaet_verkehrszaehldaten"
FAILURE_THRESHOLD = 1
EXECUTION_TIMEOUT = timedelta(minutes=60)
SCHEDULE = "0 6 * * *"

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
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_09": Variable.get("FTP_USER_09"),
            "FTP_PASS_09": Variable.get("FTP_PASS_09"),
        },
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/MOB-StatA/Dbdstcsvexport",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    ods_publish = DockerOperator(
        task_id="ods-publish",
        image="ghcr.io/opendatabs/data-processing/ods_publish:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl_id 100006,100013,100356",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--ods_publish",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    rsync = DockerOperator(
        task_id="rsync",
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files mobilitaet_verkehrszaehldaten.json",
        container_name=f"{DAG_ID}--rsync",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source="/data/dev/workspace", target="/code", type="bind"),
        ],
    )

    upload >> ods_publish
    upload >> rsync
