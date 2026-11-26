"""
# dcc_ki_grosserrat.py
The Grosser Rat for the MCP server, in one DAG:

- docling: converts the PDFs of the documents and transcripts into Parlament/markdown, docling and images;
  the Markdown is rsynced.
- topics: gives every business item a topic, in Parlament/topics/topics.csv.
- summaries: lets Gemma read the documents, in Parlament/summaries/extract.db. It takes the document list
  from the corpus-mcp.db of the previous run.
- mcp: builds the database of the MCP server in Parlament/mcp/, which is rsynced.
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

PARLAMENT = "/mnt/OGD-DataExch/StatA/Parlament"
CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt"

PROXY_ENV_VARS = {
    "HTTP_PROXY": Variable.get("http_proxy"),
    "HTTPS_PROXY": Variable.get("https_proxy"),
    "NO_PROXY": Variable.get("no_proxy"),
    "CURL_CA_BUNDLE": CA_BUNDLE,
    "NODE_EXTRA_CA_CERTS": CA_BUNDLE,
    "PERL_LWP_SSL_CA_FILE": CA_BUNDLE,
    "REQUESTS_CA_BUNDLE": CA_BUNDLE,
    "SSL_CERT_FILE": CA_BUNDLE,
}

CERT_MOUNTS = [
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
]

SSH_KEY_MOUNT = Mount(
    source="/home/syncuser/.ssh/id_rsa", target="/root/.ssh/id_rsa", type="bind"
)

default_args = {
    "owner": "thomas.gerig",
    "depend_on_past": False,
    "start_date": datetime(2026, 11, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


def job(task_id, image, environment, mounts, **kwargs):
    return DockerOperator(
        task_id=task_id,
        image=f"ghcr.io/opendatabs/data-processing/{image}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="sh -c 'update-ca-certificates || true; uv run -m etl'",
        private_environment={**COMMON_ENV_VARS, **PROXY_ENV_VARS, **environment},
        container_name=image,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[*CERT_MOUNTS, *mounts],
        **kwargs,
    )


def rsync(task_id, config, container_name, mounts):
    return DockerOperator(
        task_id=task_id,
        image="ghcr.io/opendatabs/rsync:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command=f"python3 -m rsync.sync_files {config}",
        container_name=container_name,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[SSH_KEY_MOUNT, *mounts],
    )


def parlament(folder, target, read_only=False):
    return Mount(
        source=f"{PARLAMENT}/{folder}", target=target, type="bind", read_only=read_only
    )


with DAG(
    "dcc_ki_grosserrat",
    default_args=default_args,
    description="Run the Grosser Rat jobs for the MCP server",
    schedule="15 1 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    docling = job(
        "docling",
        "dcc_grosserrat_docling",
        {
            "DOCLING_HTTP_CLIENT": Variable.get("DOCLING_HTTP_CLIENT"),
            "DOCLING_API_KEY": Variable.get("DOCLING_API_KEY"),
        },
        [
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/parlamentsdienst_grosserrat/data/export",
                target="/code/data_orig",
                type="bind",
                read_only=True,
            ),
            parlament("markdown", "/code/data/markdown"),
            parlament("docling", "/code/data/docling"),
            parlament("images", "/code/data/images"),
        ],
    )

    rsync_markdown = rsync(
        "rsync_markdown",
        "dcc_grosserrat_markdown.json",
        "dcc_grosserrat_markdown--rsync",
        [
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(source=PARLAMENT, target="/code/data", type="bind"),
        ],
    )

    topics = job(
        "topics",
        "dcc_grosserrat_topics",
        {
            "QWEN_EMBED_URL": Variable.get("QWEN_EMBED_URL"),
            "QWEN_EMBED_MODEL": "Qwen/Qwen3-Embedding-4B",
            "QWEN_EMBED_API_KEY": Variable.get("QWEN_EMBED_API_KEY", default_var=""),
        },
        [
            parlament("markdown", "/code/data_orig/markdown", read_only=True),
            parlament("topics", "/code/data"),
        ],
        owner="thomas.gerig",
    )

    summaries = job(
        "summaries",
        "dcc_grosserrat_summaries",
        {
            "GEMMA_URL": GEMMA_URL,
            "GEMMA_MODEL": Variable.get("GEMMA_MODEL"),
            "GEMMA_API_KEY": Variable.get("GEMMA_API_KEY", default_var=""),
        },
        [
            parlament("markdown", "/code/data_orig/markdown", read_only=True),
            parlament("docling", "/code/data_orig/docling", read_only=True),
            parlament("mcp", "/code/data_orig/mcp", read_only=True),
            parlament("summaries", "/code/data"),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/dcc_grosserrat_summaries/tmp",
                target="/code/tmp",
                type="bind",
            ),
        ],
        owner="thomas.gerig",
    )

    mcp = job(
        "mcp",
        "dcc_grosserrat_mcp",
        {"MCP_EMBED_THREADS": "8"},
        [
            parlament("markdown", "/code/data_orig/markdown", read_only=True),
            parlament("docling", "/code/data_orig/docling", read_only=True),
            parlament("topics", "/code/data_orig/topics", read_only=True),
            parlament("summaries", "/code/data_orig/summaries", read_only=True),
            parlament("mcp", "/code/data"),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/dcc_grosserrat_mcp/tmp",
                target="/code/tmp",
                type="bind",
            ),
        ],
        owner="thomas.gerig",
    )

    rsync_mcp = rsync(
        "rsync_mcp",
        "dcc_grosserrat_mcp.json",
        "dcc_grosserrat_mcp--rsync",
        [
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(source=PARLAMENT, target="/code/data", type="bind"),
        ],
    )

    docling >> [rsync_markdown, topics, summaries]
    [topics, summaries] >> mcp >> rsync_mcp
