"""
# parlamentsdienst_grosserrat_summaries.py
Lets Gemma read the documents of the Grosser Rat: a summary, search terms and quoted relations per
document, in Parlament/summaries/extract.db. Reads the Markdown and Docling JSON of
parlamentsdienst_grosserrat_datasette and the document list in the corpus-mcp.db of
parlamentsdienst_grosserrat_mcp, which takes the results into the database of the MCP server.
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
# Temporary, for trying out: the KIS test gateway until the Variable names it; a URL that already does is left as it is.
GEMMA_URL = Variable.get("GEMMA_URL")
if ".intranet.test." not in GEMMA_URL:
    GEMMA_URL = GEMMA_URL.replace(".intranet.", ".intranet.test.", 1)

default_args = {
    "owner": "thomas.gerig",
    "depend_on_past": False,
    "start_date": datetime(2026, 11, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    "parlamentsdienst_grosserrat_summaries",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat_summaries docker container",
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id="upload",
        image="ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat_summaries:latest",
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
            "GEMMA_URL": GEMMA_URL,
            "GEMMA_MODEL": Variable.get("GEMMA_MODEL"),
            "GEMMA_API_KEY": Variable.get("GEMMA_API_KEY", default_var=""),
            "CURL_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "NODE_EXTRA_CA_CERTS": "/etc/ssl/certs/ca-certificates.crt",
            "PERL_LWP_SSL_CA_FILE": "/etc/ssl/certs/ca-certificates.crt",
            "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
            "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        },
        container_name="parlamentsdienst_grosserrat_summaries",
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
                source="/mnt/OGD-DataExch/StatA/Parlament/docling",
                target="/code/data_orig/docling",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Parlament/mcp",
                target="/code/data_orig/mcp",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Parlament/summaries",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat_summaries/tmp",
                target="/code/tmp",
                type="bind",
            ),
        ],
    )
