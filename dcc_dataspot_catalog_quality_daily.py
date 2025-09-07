"""
# dcc_dataspot_catalog_quality_daily
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator

from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "renato.farruggio",
    "description": "Run dataspot catalog quality daily checks",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 8),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dcc_dataspot_catalog_quality_daily",
    default_args=default_args,
    description="Run dataspot catalog quality daily checks",
    schedule="0 4 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command='''
        docker rm -f dcc_dataspot_catalog_quality_daily 2>/dev/null || true
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
    
    # Task: ensure catalog quality as defined in dataspot
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
    
    # Set the task dependency
    cleanup_containers >> catalog_quality_daily
