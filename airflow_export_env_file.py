"""
# airflow_export_variables
This DAG exports all Airflow Variables into a .env file inside a mounted directory
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.settings import Session
from docker.types import Mount

# Fetch path to mount from Airflow Variables
PATH_TO_CREDENTIALS = Variable.get("PATH_TO_CREDENTIALS")

# Open DB session to fetch all Variables
session = Session()
# Fetch all variables and EXCLUDE those starting with 'PATH_TO'
all_vars = {var.key: var.val for var in session.query(Variable).all() if not var.key.startswith("PATH_TO")}
session.close()

# Define default arguments for the DAG
default_args = {
    "owner": "orhan.saeedi",
    "description": "Export Airflow Variables to a .env file",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": Variable.get("EMAIL_RECEIVERS"),
}

# Define the DAG
with DAG(
    "airflow_export_variables",
    default_args=default_args,
    description="DockerOperator to export Airflow Variables into a .env file",
    schedule_interval=None,
    catchup=False,
) as dag:

    export_env_vars = DockerOperator(
        task_id="export_variables_to_env",
        image="apache/airflow:2.7.2",
        container_name="airflow_export_env_container",
        api_version="auto",
        auto_remove="force",
        user="0",
        command="bash -c \"mkdir -p /opt/airflow/credentials && printenv | sort > /opt/airflow/credentials/.env && cat /opt/airflow/credentials/.env\"",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_default",
        private_environment=all_vars,
        mounts=[
            Mount(
                source=PATH_TO_CREDENTIALS,
                target="/opt/airflow/credentials",
                type="bind",
            ),
        ],
    )
