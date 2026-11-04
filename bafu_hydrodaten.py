"""
# bafu_hydrodaten
This DAG updates the following datasets:

- [100089](https://data.bs.ch/explore/dataset/100089)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "bafu_hydrodaten"
FAILURE_THRESHOLD = 11  # Skip first 11 failures, fail on the 12th consecutive failure
EXECUTION_TIMEOUT = timedelta(minutes=2)
SCHEDULE = "*/5 * * * *"

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 22),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
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
        bash_command=f"""
            docker rm -f {DAG_ID} 2>/dev/null || true
            """,
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
        command="uv run -m etl_https",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTPS_URL_01": Variable.get("HTTPS_URL_01"),
            "HTTPS_USER_01": Variable.get("HTTPS_USER_01"),
            "HTTPS_PASS_01": Variable.get("HTTPS_PASS_01"),
            "RHEIN_FILES": Variable.get("RHEIN_FILES"),
            "BIRS_FILES": Variable.get("BIRS_FILES"),
            "WIESE_FILES": Variable.get("WIESE_FILES"),
            "RHEIN_KLINGENTHAL_FILES": Variable.get("RHEIN_KLINGENTHAL_FILES"),
            "ODS_PUSH_URL_100089": Variable.get("ODS_PUSH_URL_100089"),
            "ODS_PUSH_URL_100236": Variable.get("ODS_PUSH_URL_100236"),
            "ODS_PUSH_URL_100235": Variable.get("ODS_PUSH_URL_100235"),
            "ODS_PUSH_URL_100243": Variable.get("ODS_PUSH_URL_100243"),
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
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    cleanup_containers >> upload
