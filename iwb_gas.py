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
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

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
    "iwb_gas",
    default_args=default_args,
    description="Run the iwb_gas docker container",
    schedule="0 13 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/iwb_gas:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_04": Variable.get("FTP_USER_04"),
            "FTP_PASS_04": Variable.get("FTP_PASS_04"),
        },
        container_name="iwb_gas--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/iwb_gas/data",
                target="/code/data",
                type="bind",
            )
        ],
    )

    fit_model = DockerOperator(
        task_id="fit_model",
        image="gasverbrauch:latest",
        api_version="auto",
        auto_remove="force",
        mnt_tmp_dir=False,
        command="Rscript /code/data-processing/stata_erwarteter_gasverbrauch/Gasverbrauch_OGD.R",
        container_name="gasverbrauch--fit_model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing",
                target="/code/data-processing",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Gasverbrauch",
                target="/code/data-processing/stata_erwarteter_gasverbrauch/data/export",
                type="bind",
            ),
        ],
    )

    upload >> fit_model
