"""
# parlamentsdienst_grosserrat.py
This DAG updates the following datasets:

- [100307](https://data.bs.ch/explore/dataset/100307)
- [100308](https://data.bs.ch/explore/dataset/100308)
- [100309](https://data.bs.ch/explore/dataset/100309)
- [100310](https://data.bs.ch/explore/dataset/100310)
- [100311](https://data.bs.ch/explore/dataset/100311)
- [100312](https://data.bs.ch/explore/dataset/100312)
- [100313](https://data.bs.ch/explore/dataset/100313)
- [100314](https://data.bs.ch/explore/dataset/100314)
- [100347](https://data.bs.ch/explore/dataset/100347)
- [100348](https://data.bs.ch/explore/dataset/100348)
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
    "start_date": datetime(2024, 2, 2),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "parlamentsdienst_grosserrat",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat docker container",
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name="parlamentsdienst_grosserrat",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
