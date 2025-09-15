"""
# iwb_netzlast.py
This DAG updates the following datasets:

- [100233](https://data.bs.ch/explore/dataset/100233)
- [100245](https://data.bs.ch/explore/dataset/100245)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "iwb_netzlast"
EXECUTION_TIMEOUT = timedelta(minutes=50)
SCHEDULE = "0 * * * *"

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 25),
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
    upload = DockerOperator(
        task_id="upload",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        execution_timeout=EXECUTION_TIMEOUT,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/IWB/Netzlast",
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
        image="ghcr.io/opendatabs/stata_erwarteter_stromverbrauch:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="Rscript Stromverbrauch_OGD.R",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--fit_model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/stata_erwarteter_stromverbrauch/data",
                target="/home/rstudio/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Stromverbrauch",
                target="/home/rstudio/data/export",
                type="bind",
            ),
        ],
    )

    upload >> fit_model
