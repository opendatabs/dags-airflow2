"""
# airflow_export_variables
This DAG exports only Airflow Variables (not system env) into a .env file
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
# Fetch all Variables and exclude PATH_TO variables
all_vars = {
    var.key: var.val
    for var in session.query(Variable).all()
    if not var.key.startswith("PATH_TO")
    and var.key not in {"http_proxy", "https_proxy"}
}
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

# Build the command dynamically to echo only your Variables
echo_commands = " && ".join(
    [f'echo "{key}=\\"${key}\\"" >> /opt/airflow/credentials/.env' for key in all_vars.keys()]
)

full_command = f'bash -c "mkdir -p /opt/airflow/credentials && rm -f /opt/airflow/credentials/.env && {echo_commands} && cat /opt/airflow/credentials/.env"'

# Define the DAG
with DAG(
    "airflow_export_variables",
    default_args=default_args,
    description="DockerOperator to export Airflow Variables into a .env file",
    schedule=None,
    catchup=False,
) as dag:

    export_env_vars = DockerOperator(
        task_id="export_variables_to_env",
        image="apache/airflow:2.7.2",
        container_name="airflow_export_env_container",
        api_version="auto",
        auto_remove="force",
        user="0",  # Important for writing into mounted volume
        command=full_command,
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
