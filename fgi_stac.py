"""
# fgi_stac

STAC/Dataspot → Katalog → GeoJSON/Schema → HUWISE (FTP).

| Task | Befehl |
|------|--------|
| sync_catalog | `uv run sync_catalog.py` |
| prepare_assets | `uv run prepare_assets.py` |
| publish | `uv run publish.py` |

Vollpipeline im Container: `uv run etl.py`
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from docker.types import Mount
from helpers.failure_tracking_operator import FailureTrackingDockerOperator

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

DAG_ID = "fgi_stac"
FAILURE_THRESHOLD = 1  # Skip first failure, fail on second.
EXECUTION_TIMEOUT = timedelta(minutes=1440)
SCHEDULE = None

TASK_IDS = ("sync_catalog", "prepare_assets", "publish")

default_args = {
    "owner": "rstam.aloush",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 30),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

FGI_STAC_ENV = {
    **COMMON_ENV_VARS,
    "API_KEY_MAPBS": Variable.get("API_KEY_MAPBS"),
    "DATASPOT_EXPOSED_CLIENT_ID": Variable.get("DATASPOT_EXPOSED_CLIENT_ID"),
    "DATASPOT_TENANT_ID": Variable.get("DATASPOT_TENANT_ID"),
    "DATASPOT_CLIENT_ID": Variable.get("DATASPOT_CLIENT_ID"),
    "DATASPOT_CLIENT_SECRET": Variable.get("DATASPOT_CLIENT_SECRET"),
    "DATASPOT_SERVICE_USER_ACCESS_KEY": Variable.get("DATASPOT_SERVICE_USER_ACCESS_KEY"),
}

FGI_STAC_MOUNTS = [
    Mount(
        source="/mnt/OGD-DataExch/StatA/FGI/STAC",
        target="/code/data",
        type="bind",
    ),
    Mount(
        source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data_orig",
        target="/code/data_orig",
        type="bind",
    ),
    Mount(
        source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
        target="/code/change_tracking",
        type="bind",
    ),
]


def fgi_stac_docker_task(*, task_id: str, command: str) -> FailureTrackingDockerOperator:
    return FailureTrackingDockerOperator(
        task_id=task_id,
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command=command,
        private_environment=FGI_STAC_ENV,
        container_name=f"{DAG_ID}--{task_id}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=FGI_STAC_MOUNTS,
    )


with DAG(
    dag_id=DAG_ID,
    description=f"Run the {DAG_ID} pipeline (catalog → prepare → publish)",
    default_args=default_args,
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command="\n".join(
            f"docker rm -f {DAG_ID}--{task_id} 2>/dev/null || true" for task_id in TASK_IDS
        ),
    )

    sync_catalog = fgi_stac_docker_task(
        task_id="sync_catalog",
        command="uv run sync_catalog.py",
    )
    prepare_assets = fgi_stac_docker_task(
        task_id="prepare_assets",
        command="uv run prepare_assets.py",
    )
    publish = fgi_stac_docker_task(
        task_id="publish",
        command="uv run publish.py",
    )

    cleanup_containers >> sync_catalog >> prepare_assets >> publish
