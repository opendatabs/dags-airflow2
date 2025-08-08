"""
# dcc_dataspot_sync_org_structures_and_ods_datasets
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator

from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "renato.farruggio",
    "description": "Run dataspot sync operations in sequence",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 5),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dcc_dataspot_daily_jobs",
    default_args=default_args,
    description="Run dataspot sync operations in sequence",
    schedule="0 3 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command='''
        docker rm -f dcc_dataspot_catalog_quality_daily 2>/dev/null || true
        docker rm -f dcc_dataspot_sync_org_structures 2>/dev/null || true
        docker rm -f dcc_dataspot_sync_ods_dataset_components 2>/dev/null || true
        docker rm -f dcc_dataspot_sync_ods_datasets 2>/dev/null || true
        ''',
    )
    
    # Common environment variables for tasks
    dataspot_env = {
        **COMMON_ENV_VARS,
        "DATASPOT_EMAIL_RECEIVERS": Variable.get("DATASPOT_EMAIL_RECEIVERS"),
        "DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY": Variable.get("DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY"),
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
    
    # First task: ensure catalog quality as defined in dataspot
    catalog_quality_daily = DockerOperator(
        task_id="catalog_quality_daily",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        private_environment=dataspot_env,
        command="python -m scripts.catalog_quality_daily.daily_checks__combined",
        container_name="dcc_dataspot_catalog_quality_daily",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Second task: sync organization structures
    sync_org_structures = DockerOperator(
        task_id="sync_org_structures",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        private_environment=dataspot_env,
        command="python -m scripts.sync_org_structures",
        container_name="dcc_dataspot_sync_org_structures",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Third task: sync ODS dataset components
    sync_ods_dataset_components = DockerOperator(
        task_id="sync_ods_dataset_components",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        private_environment=dataspot_env,
        command="python -m scripts.sync_ods_dataset_components",
        container_name="dcc_dataspot_sync_ods_dataset_components",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    # Fourth task: sync ODS datasets
    sync_ods_datasets = DockerOperator(
        task_id="sync_ods_datasets",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        private_environment=dataspot_env,
        command="python -m scripts.sync_ods_datasets",
        container_name="dcc_dataspot_sync_ods_datasets",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Set the task dependency
    cleanup_containers >> catalog_quality_daily >> sync_org_structures >> sync_ods_dataset_components >> sync_ods_datasets
