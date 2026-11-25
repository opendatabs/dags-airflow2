"""
# stata_superblockbefragung
This DAG updates the following datasets:

- [100558](https://data.bs.ch/explore/dataset/100558)
- [100559](https://data.bs.ch/explore/dataset/100559)
- [100560](https://data.bs.ch/explore/dataset/100560)
- [100561](https://data.bs.ch/explore/dataset/100561)
- [100562](https://data.bs.ch/explore/dataset/100562)
- [100563](https://data.bs.ch/explore/dataset/100563)
- [100564](https://data.bs.ch/explore/dataset/100564)
- [100565](https://data.bs.ch/explore/dataset/100565)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "stata_superblockbefragungen"
SCHEDULE = None

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2026, 11, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    DAG_ID,
    description=f"Run the {DAG_ID} docker container",
    default_args=default_args,
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
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_befragungen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Befragungen/Superblockbefragungen_Ablage_StatA",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Befragungen/Superblockbefragungen_OGD",
                target="/code/data",
                type="bind",
            ),
        ],
    )
