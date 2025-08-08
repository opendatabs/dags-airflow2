"""
# mobilitaet_mikromobilitaet_stats
This DAG updates the following datasets:

- [100416](https://data.bs.ch/explore/dataset/100416)
- [100418](https://data.bs.ch/explore/dataset/100418)
- [100422](https://data.bs.ch/explore/dataset/100422)
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
    "start_date": datetime(2025, 2, 12),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "mobilitaet_mikromobilitaet_stats",
    description="Run the mobilitaet_mikromobilitaet_stats",
    default_args=default_args,
    schedule="0 4 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id="process-upload",
        image="ghcr.io/opendatabs/data-processing/mobilitaet_mikromobilitaet_stats:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="mobilitaet_mikromobilitaet_stats",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/BVD-MOB/Mikromobilitaet",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/mobilitaet_mikromobilitaet_stats/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
