"""
# fgi_stac
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from docker.types import Mount
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "fgi_stac"
FAILURE_THRESHOLD = 1  # Skip first failure, fail on second.
EXECUTION_TIMEOUT = timedelta(minutes=90)
SCHEDULE = "0 * * * *"

default_args = {
    "owner": "rstam.aloush",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 30),
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
        bash_command=f"""
            docker rm -f {DAG_ID}--upload 2>/dev/null || true
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
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "API_KEY_MAPBS": Variable.get("API_KEY_MAPBS"),
            "DATASPOT_EXPOSED_CLIENT_ID": Variable.get("DATASPOT_EXPOSED_CLIENT_ID"),
            "DATASPOT_TENANT_ID": Variable.get("DATASPOT_TENANT_ID"),
            "DATASPOT_CLIENT_ID": Variable.get("DATASPOT_CLIENT_ID"),
            "DATASPOT_CLIENT_SECRET": Variable.get("DATASPOT_CLIENT_SECRET"),
            "DATASPOT_SERVICE_USER_ACCESS_KEY": Variable.get("DATASPOT_SERVICE_USER_ACCESS_KEY"),
        },
        container_name=f"{DAG_ID}--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/FGI/STAC",
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
