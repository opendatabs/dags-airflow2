"""
# dcc_dataspot_sync_org_structures_and_ods_datasets
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

default_args = {
    "owner": "renato.farruggio",
    "description": "Run dataspot sync operations in sequence",
    "depend_on_past": False,
    "start_date": datetime(2025, 6, 5),
    "email": Variable.get("DATASPOT_EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dcc_dataspot_sync_org_structures_and_ods_datasets",
    default_args=default_args,
    description="Run dataspot sync operations in sequence",
    schedule_interval="0 3 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    # Common environment variables for both tasks
    dataspot_env = {
        **COMMON_ENV_VARS,
        "DATASPOT_EMAIL_RECEIVERS": Variable.get("DATASPOT_EMAIL_RECEIVERS"),
        "DATASPOT_EMAIL_SERVER": Variable.get("DATASPOT_EMAIL_SERVER"),
        "DATASPOT_EMAIL_SENDER": Variable.get("DATASPOT_EMAIL_SENDER"),
        "DATASPOT_ADMIN_USERNAME": Variable.get("DATASPOT_ADMIN_USERNAME"),
        "DATASPOT_ADMIN_PASSWORD": Variable.get("DATASPOT_ADMIN_PASSWORD"),
        "DATASPOT_EDITOR_USERNAME": Variable.get("DATASPOT_EDITOR_USERNAME"),
        "DATASPOT_EDITOR_PASSWORD": Variable.get("DATASPOT_EDITOR_PASSWORD"),
        "DATASPOT_CLIENT_ID": Variable.get("DATASPOT_CLIENT_ID"),
        "DATASPOT_AUTHENTICATION_TOKEN_URL": Variable.get("DATASPOT_AUTHENTICATION_TOKEN_URL"),
        "DATASPOT_API_BASE_URL": Variable.get("DATASPOT_API_BASE_URL"),
        "ODS_DOMAIN": Variable.get("ODS_DOMAIN"),
        "ODS_API_TYPE": Variable.get("ODS_API_TYPE")
    }
    
    # First task: sync organization structures
    sync_org_structures = DockerOperator(
        task_id="sync_org_structures",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        private_environment=dataspot_env,
        command="python -m scripts.sync_org_structures",
        container_name="dcc_dataspot_sync_org_structures",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Second task: sync ODS datasets
    sync_ods_datasets = DockerOperator(
        task_id="sync_ods_datasets",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        private_environment=dataspot_env,
        command="python -m scripts.sync_ods_datasets",
        container_name="dcc_dataspot_sync_ods_datasets",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Set the task dependency
    sync_org_structures >> sync_ods_datasets
