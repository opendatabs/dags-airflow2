"""
# kapo_eventverkehr_stjakob
This DAG updates the following datasets:

- [100419](https://data.bs.ch/explore/dataset/100419)
- [100429](https://data.bs.ch/explore/dataset/100429)
- [100464](https://data.bs.ch/explore/dataset/100464)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "kapo_eventverkehr_stjakob"
FAILURE_THRESHOLD = 0  # Immediate failure with no skipping
EXECUTION_TIMEOUT = timedelta(minutes=10)
SCHEDULE = "*/5 * * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 1, 31),
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

    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command=f'''
            docker rm -f {DAG_ID} 2>/dev/null || true
            ''',
    )

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
            "SHAREPOINT_TENANT_ID": Variable.get("SHAREPOINT_TENANT_ID"),
            "SHAREPOINT_CLIENT_ID": Variable.get("SHAREPOINT_CLIENT_ID"),
            "SHAREPOINT_HOST": Variable.get("SHAREPOINT_HOST"),
            "SHAREPOINT_SITE_NAME_KAPO_EVENTVERKEHR_STJAKOB": Variable.get(
                "SHAREPOINT_SITE_NAME_KAPO_EVENTVERKEHR_STJAKOB"
            ),
            "SHAREPOINT_CERT_PATH": Variable.get("SHAREPOINT_CERT_PATH"),
            "SHAREPOINT_THUMBPRINT": Variable.get("SHAREPOINT_THUMBPRINT"),
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
                 source="/mnt/OGD-DataExch/KaPo-Eventverkehr-St.Jakob",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source=Variable.get("PATH_TO_CERTS"),
                target="/certs",
                type="bind",
            ),
        ],
    )

    cleanup_containers >> upload
