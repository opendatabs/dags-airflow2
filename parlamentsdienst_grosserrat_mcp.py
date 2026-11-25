"""
# parlamentsdienst_grosserrat_mcp.py
Prepares everything the MCP server of the Grosser Rat reads, in Parlament/mcp/: corpus-mcp.db (index,
graph, topics and the text of each document), vectors.db, vectors.faiss and release.json. Reads the
Markdown and Docling JSON of parlamentsdienst_grosserrat_datasette and the topics.csv of
parlamentsdienst_grosserrat_topics; Gemma reads the new documents. failed.csv lists what failed.
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
    "start_date": datetime(2026, 11, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "parlamentsdienst_grosserrat_mcp",
    default_args=default_args,
    description="Run the parlamentsdienst_grosserrat_mcp docker containers",
    schedule=None,
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    mcp_env = {
        **COMMON_ENV_VARS,
        "HTTP_PROXY": Variable.get("http_proxy"),
        "HTTPS_PROXY": Variable.get("https_proxy"),
        "NO_PROXY": Variable.get("no_proxy"),
        "GEMMA_URL": Variable.get("GEMMA_URL"),
        "GEMMA_MODEL": Variable.get("GEMMA_MODEL"),
        "GEMMA_API_KEY": Variable.get("GEMMA_API_KEY", default_var=""),
        "EXTRACT_MCP_DB": "/code/data/corpus-mcp.db",
        "MCP_EXTRACT_DB": "/code/data/extract.db",
        "MCP_EMBED_THREADS": "8",
        "CURL_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
        "NODE_EXTRA_CA_CERTS": "/etc/ssl/certs/ca-certificates.crt",
        "PERL_LWP_SSL_CA_FILE": "/etc/ssl/certs/ca-certificates.crt",
        "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    }

    mcp_mounts = [
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
            source="/mnt/OGD-DataExch/StatA/Parlament/topics",
            target="/code/data_orig/topics",
            type="bind",
            read_only=True,
        ),
        Mount(
            source="/mnt/OGD-DataExch/StatA/Parlament/mcp",
            target="/code/data",
            type="bind",
        ),
    ]

    # Mirrors the open data, builds corpus-mcp.db and embeds the passages of new documents.
    reindex = DockerOperator(
        task_id="reindex",
        image="ghcr.io/dcc-bs/grossrat-bs-mcp-job:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; /code/mcp/.venv/bin/corpus-mcp reindex'",
        private_environment=mcp_env,
        container_name="parlamentsdienst_grosserrat_mcp_reindex",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=mcp_mounts,
    )

    # Gemma reads the new documents and writes their search terms into extract.db.
    read = DockerOperator(
        task_id="read",
        image="ghcr.io/dcc-bs/grossrat-bs-mcp-job:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; /code/extract/.venv/bin/python /code/extract/etl.py'",
        private_environment=mcp_env,
        container_name="parlamentsdienst_grosserrat_mcp_read",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=mcp_mounts,
    )

    # Takes Gemma's readings into corpus-mcp.db, writes release.json and failed.csv.
    release = DockerOperator(
        task_id="release",
        image="ghcr.io/dcc-bs/grossrat-bs-mcp-job:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; /code/mcp/.venv/bin/corpus-mcp reindex --no-mirror'",
        private_environment=mcp_env,
        container_name="parlamentsdienst_grosserrat_mcp_release",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=mcp_mounts,
    )

    reindex >> read >> release
