"""
# parlamentsdienst_grosserrat_datasette.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

PATH_TO_LOCAL_CERTS = Variable.get("PATH_TO_LOCAL_CERTS")
CA_ZID_FILENAME = Variable.get("CA_ZID_FILENAME")
CA_PKI_FILENAME = Variable.get("CA_PKI_FILENAME")

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 5, 8),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "parlamentsdienst_grosserrat_datasette",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat_datasette docker container",
    schedule="0 5 * * 0",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat_datasette:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; uv run -m etl'",
        # For debugging
        # command="bash -lc 'trap : TERM INT; sleep 36000'",
        private_environment={
            **COMMON_ENV_VARS,
            "DOCLING_HTTP_CLIENT": Variable.get("DOCLING_HTTP_CLIENT"),
            "DOCLING_API_KEY": Variable.get("DOCLING_API_KEY"),
            "CURL_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "NODE_EXTRA_CA_CERTS": "/etc/ssl/certs/ca-certificates.crt",
            "PERL_LWP_SSL_CA_FILE": "/etc/ssl/certs/ca-certificates.crt",
            "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        },
        container_name="parlamentsdienst_grosserrat_datasette",
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
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
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat/data/export",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat_datasette/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"/mnt/OGD-DataExch/StatA/Parlament/markdown",
                target="/code/data/markdown",
                type="bind",
            ),
            Mount(
                source=f"/mnt/OGD-DataExch/StatA/Parlament/text",
                target="/code/data/text",
                type="bind",
            ),
        ],
    )

    rsync = DockerOperator(
        task_id="rsync",
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python3 -m rsync.sync_files parlamentsdienst_grosserrat.json",
        container_name="parlamentsdienst_grosserrat--rsync",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source="/data/dev/workspace", target="/code", type="bind"),
        ],
    )

    upload >> rsync
