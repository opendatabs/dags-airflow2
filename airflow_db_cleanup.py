"""
# airflow_db_cleanup
This DAG removes every DAG-run which is older than 90 days
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


PATH_TO_CODE = Variable.get("PATH_TO_CODE")

# Define default arguments for the DAG
default_args = {
    "owner": "orhan.saeedi",
    "description": "Run the airflow db clean command",
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
    "airflow_db_cleanup",
    default_args=default_args,
    description="DockerOperator to clean up old Airflow task runs and logs",
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    # DockerOperator to run the db cleanup command
    cleanup_db = DockerOperator(
        task_id="cleanup_old_task_runs",
        image="apache/airflow:2.7.2",
        container_name="airflow_cleanup_container",
        api_version="auto",
        auto_remove=True,
        command="bash -c \"airflow db clean --yes --clean-before-timestamp $(date -d '90 days ago' +%Y-%m-%d)\"",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_default",
        environment={
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": Variable.get(
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
            )
        },
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/dags-airflow2",
                target="/opt/airflow/dags",
                type="bind",
            ),
            Mount(source="/data/airflow/logs", target="/opt/airflow/logs", type="bind"),
            Mount(
                source="/data/airflow/plugins",
                target="/opt/airflow/plugins",
                type="bind",
            ),
        ],
    )
