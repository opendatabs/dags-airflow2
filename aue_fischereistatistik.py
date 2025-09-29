"""
# aue-umweltlabor
This DAG updates the following datasets:

- [100066](https://data.bs.ch/explore/dataset/100066)
- [100067](https://data.bs.ch/explore/dataset/100067)
- [100068](https://data.bs.ch/explore/dataset/100068)
- [100069](https://data.bs.ch/explore/dataset/100069)
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
    "start_date": datetime(2025, 9, 29),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    "aue_fischereistatistik",
    description="Run the aue_fischereistatistik docker container",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    create_gewaesser_file = DockerOperator(
        task_id="create-gewaesser-file",
        image="ghcr.io/opendatabs/data-processing/aue_fischereistatistik:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m gewaesser",
        private_environment=COMMON_ENV_VARS,
        container_name="aue_fischereistatistik-gewaesser",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/AUE-FA/Fischereistatistik/OGD",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    upload_fischereistatistk = DockerOperator(
        task_id="upload-fischereistatistk",
        image="ghcr.io/opendatabs/data-processing/aue_fischereistatistik:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="aue_fischereistatistik-fischereistatistik",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/AUE-FA/Fischereistatistik/OGD",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/AUE-FA/Fischereistatistik/csv_rohdaten",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/aue_fischereistatistik/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
