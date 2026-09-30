"""
# iwb_gas.py
This DAG updates the following datasets:

- [100304](https://data.bs.ch/explore/dataset/100304)
- [100353](https://data.bs.ch/explore/dataset/100353)

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "iwb_gas"
FAILURE_THRESHOLD = 1
EXECUTION_TIMEOUT = timedelta(minutes=60)
SCHEDULE = "0 13 * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
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
            "FTP_USER_04": Variable.get("FTP_USER_04"),
            "FTP_PASS_04": Variable.get("FTP_PASS_04"),
        },
        container_name=f"{DAG_ID}--upload",
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

    fit_model = DockerOperator(
        task_id="fit_model",
        image="ghcr.io/opendatabs/stata_erwarteter_gasverbrauch:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="Rscript Gasverbrauch_OGD.R",
        private_environment=COMMON_ENV_VARS,
        user="root",
        container_name="gasverbrauch--fit_model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/stata_erwarteter_gasverbrauch/data",
                target="/home/rstudio/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Gasverbrauch",
                target="/home/rstudio/data/export",
                type="bind",
            ),
        ],
    )

    upload >> fit_model
