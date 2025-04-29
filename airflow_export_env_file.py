"""
# airflow_export_env_file
This DAG exports the variables to an env file to run jobs locally
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

# Fetch all variables
all_vars = Variable.get_variable_from_secrets()
env_vars = {k: v for k, v in all_vars.items() if not k.startswith('PATH_TO_')}

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 2, 26),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="export_all_airflow_vars_to_env",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    export_env = DockerOperator(
        task_id="export_env_file",
        image="python:3.12-slim",
        environment=env_vars,
        command="bash -c 'printenv > /code/.env && cat /code/.env'",
        auto_remove="force",
        mount_tmp_dir=False,
        container_name="export_env_file",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_default",
        tty=True,
        do_xcom_push=False,
        mounts=[
            Mount(
                source=Variable.get("PATH_TO_CREDENTIALS"),
                target="/code",
                type="bind",
            )
        ]
    )
