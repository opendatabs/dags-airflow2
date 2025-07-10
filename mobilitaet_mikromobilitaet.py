"""
# mobilitaet_mikromobilitaet
This DAG updates the following datasets:

- [100415](https://data.bs.ch/explore/dataset/100415)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "mobilitaet_mikromobilitaet"
FAILURE_THRESHOLD = 3
EXECUTION_TIMEOUT = timedelta(minutes=5)
SCHEDULE = "*/10 * * * *"


def check_manual_triggering(**context):
    dag_run: DagRun = context.get("dag_run")
    # Below condition will return true if DAG is triggered manually.
    if dag_run.external_trigger:
        raise RuntimeError(
            "Manual DAG run disallowed since it is only meant to be triggered by the scheduler. "
            "For further info see job `mobilitaet_mikromobilitaet_stats` in the data-processing repository."
        )


default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 1, 31),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    manual_trigger_check = PythonOperator(
        task_id="manual_trigger_check",
        python_callable=check_manual_triggering,
    )

    process_upload = FailureTrackingDockerOperator(
        task_id="process-upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/BVD-MOB/Mikromobilitaet",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/mobilitaet_mikromobilitaet/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    manual_trigger_check >> process_upload
