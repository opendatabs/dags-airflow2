"""
# parlamentsdienst_gr_abstimmungen
This DAG updates the following datasets:

- [100186](https://data.bs.ch/explore/dataset/100186)
- [100188](https://data.bs.ch/explore/dataset/100188)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "jonas.bieri",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 12),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "parlamentsdienst_gr_abstimmungen",
    default_args=default_args,
    description="Run the parlamentsdienst_gr_abstimmungen docker container",
    schedule_interval=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_gr_abstimmungen:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "ODS_PUSH_URL_100186": Variable.get("ODS_PUSH_URL_100186"),
            "FTP_SERVER_GR": Variable.get("FTP_SERVER_GR"),
            "FTP_USER_GR_TRAKT_LIST": Variable.get("FTP_USER_GR_TRAKT_LIST"),
            "FTP_PASS_GR_TRAKT_LIST": Variable.get("FTP_PASS_GR_TRAKT_LIST"),
            "FTP_USER_GR_POLLS": Variable.get("FTP_USER_GR_POLLS"),
            "FTP_PASS_GR_POLLS": Variable.get("FTP_PASS_GR_POLLS"),
            "FTP_USER_GR_POLLS_ARCHIVE": Variable.get("FTP_USER_GR_POLLS_ARCHIVE"),
            "FTP_PASS_GR_POLLS_ARCHIVE": Variable.get("FTP_PASS_GR_POLLS_ARCHIVE"),
            "FTP_USER_GR_SESSION_DATA": Variable.get("FTP_USER_GR_SESSION_DATA"),
            "FTP_PASS_GR_SESSION_DATA": Variable.get("FTP_PASS_GR_SESSION_DATA"),
        },
        container_name="parlamentsdienst_gr_abstimmungen--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_gr_abstimmungen/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_gr_abstimmungen/data_orig",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_gr_abstimmungen/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
