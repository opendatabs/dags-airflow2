"""
# fgi_geodatenshop
This DAG updates the following datasets:

- [100395](https://data.bs.ch/explore/dataset/100395)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

PATH_TO_LOCAL_CERTS = Variable.get("PATH_TO_LOCAL_CERTS")
CA_ZID_FILENAME = Variable.get("CA_ZID_FILENAME")
CA_PKI_FILENAME = Variable.get("CA_PKI_FILENAME")

# DAG configuration
DAG_ID = "fgi_geodatenshop"
FAILURE_THRESHOLD = 1  # Skip first failure, fail on second
EXECUTION_TIMEOUT = timedelta(minutes=90)
SCHEDULE = "0 */2 * * *"

default_args = {
    "owner": "rstam.aloush",
    "depend_on_past": False,
    "start_date": datetime(2024, 9, 25),
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

    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command='''
            docker rm -f {DAG_ID}--upload 2>/dev/null || true
            docker rm -f {DAG_ID}--ods_harvest 2>/dev/null || true
            ''',
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
        command="sh -c 'update-ca-certificates || true; uv run -m etl'",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_01": Variable.get("FTP_USER_01"),
            "FTP_PASS_01": Variable.get("FTP_PASS_01"),
            "CURL_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "NODE_EXTRA_CA_CERTS": "/etc/ssl/certs/ca-certificates.crt",
            "PERL_LWP_SSL_CA_FILE": "/etc/ssl/certs/ca-certificates.crt",
            "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        },
        container_name=f"{DAG_ID}--upload",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_LOCAL_CERTS}{CA_ZID_FILENAME}",
                target=f"/usr/local/share/ca-certificates/{CA_ZID_FILENAME}",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_LOCAL_CERTS}{CA_PKI_FILENAME}",
                target=f"/usr/local/share/ca-certificates/{CA_PKI_FILENAME}",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/harvesters/FGI",
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

    ods_harvest = DockerOperator(
        task_id="ods-harvest",
        image="ghcr.io/opendatabs/data-processing/ods_harvest:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl gva-gpkg-ftp-csv",
        private_environment=COMMON_ENV_VARS,
        container_name=f"{DAG_ID}--ods_harvest",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        execution_timeout=EXECUTION_TIMEOUT,
    )

upload >> ods_harvest
