"""
# parlamentsdienst_grosserrat_topics.py
Gives every business item of the Grosser Rat a topic of the parliamentary services.
Reads the Markdown that parlamentsdienst_grosserrat_datasette writes and the classifier in
Parlament/topics/model/; writes Parlament/topics/topics.csv.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS

PATH_TO_LOCAL_CERTS = Variable.get("PATH_TO_LOCAL_CERTS")
CA_ZID_FILENAME = Variable.get("CA_ZID_FILENAME")
CA_PKI_FILENAME = Variable.get("CA_PKI_FILENAME")

default_args = {
    "owner": "thomas.gerig",
    "depend_on_past": False,
    "start_date": datetime(2026, 11, 24),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    "parlamentsdienst_grosserrat_topics",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat_topics docker container",
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat_topics:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; uv run -m etl'",
        private_environment={
            **COMMON_ENV_VARS,
            "HTTP_PROXY": Variable.get("http_proxy"),
            "HTTPS_PROXY": Variable.get("https_proxy"),
            "NO_PROXY": Variable.get("no_proxy"),
            "QWEN_EMBED_URL": Variable.get("QWEN_EMBED_URL"),
            "QWEN_EMBED_MODEL": Variable.get("QWEN_EMBED_MODEL"),
            "QWEN_EMBED_API_KEY": Variable.get("QWEN_EMBED_API_KEY", default_var=""),
            "CURL_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "NODE_EXTRA_CA_CERTS": "/etc/ssl/certs/ca-certificates.crt",
            "PERL_LWP_SSL_CA_FILE": "/etc/ssl/certs/ca-certificates.crt",
            "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        },
        container_name="parlamentsdienst_grosserrat_topics",
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
                source="/mnt/OGD-DataExch/StatA/Parlament/markdown",
                target="/code/data_orig/markdown",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Parlament/topics",
                target="/code/data",
                type="bind",
            ),
        ],
    )
