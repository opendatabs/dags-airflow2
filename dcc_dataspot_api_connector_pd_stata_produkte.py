"""
# dcc_dataspot_api_connector_pd_stata_produkte
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator

from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "lina.mehrle",
    "description": "Run import_stata_produkte to update StatA-Produkte",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 13),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dcc_dataspot_api_connector_pd_stata_produkte",
    default_args=default_args,
    description="Run import_stata_produkte to update StatA-Produkte",
    schedule="22 3 * * 1", # At 03:22 on Monday.
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    # Cleanup task to remove any old containers at the beginning
    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command="docker rm -f dcc_dataspot_api_connector_pd_stata_produkte 2>/dev/null || true",
    )
    
    # Common environment variables for tasks
    dataspot_env = {
        **COMMON_ENV_VARS,
        "DATASPOT_EMAIL_RECEIVERS": Variable.get("DATASPOT_EMAIL_RECEIVERS"),
        "DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY": Variable.get("DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY"),
        "DATASPOT_EMAIL_SERVER": Variable.get("DATASPOT_EMAIL_SERVER"),
        "DATASPOT_EMAIL_SENDER": Variable.get("DATASPOT_EMAIL_SENDER"),
        "DATASPOT_EXPOSED_CLIENT_ID": Variable.get("DATASPOT_EXPOSED_CLIENT_ID"),
        "DATASPOT_TENANT_ID": Variable.get("DATASPOT_TENANT_ID"),
        "DATASPOT_CLIENT_ID": Variable.get("DATASPOT_CLIENT_ID"),
        "DATASPOT_CLIENT_SECRET": Variable.get("DATASPOT_CLIENT_SECRET"),
        "DATASPOT_SERVICE_USER_ACCESS_KEY": Variable.get("DATASPOT_SERVICE_USER_ACCESS_KEY"),
        "STATA_PRODUKTE_SERVERNAME": Variable.get("STATA_PRODUKTE_SERVERNAME"),
        "STATA_PRODUKTE_USERNAME": Variable.get("STATA_PRODUKTE_USERNAME"),
        "STATA_PRODUKTE_PASSWORD": Variable.get("STATA_PRODUKTE_PASSWORD")
    }
    
    # Run Script
    api_connector_pd_stata_produkte = DockerOperator(
        task_id="api_connector_pd_stata_produkte",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=[
            "/data/airflow/kerberos_setup.sh:/tmp/kerberos_setup.sh:ro"
        ],
        command="sh -c '/tmp/kerberos_setup.sh && python -m scripts.databases_via_api.import_stata_produkte'",
        private_environment=dataspot_env,
        container_name="dcc_dataspot_api_connector_pd_stata_produkte",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
    
    # Set the task dependency
    cleanup_containers >> api_connector_pd_stata_produkte
