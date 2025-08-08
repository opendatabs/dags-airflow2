from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE


def should_continue(**kwargs):
    logs = kwargs["ti"].xcom_pull(task_ids="check_file_changed")
    if "NO_FILES_CHANGED" in logs:
        return False  # skip downstream
    return True  # continue downstream


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
    "stata_harvester",
    description="Run the stata_harvester docker container",
    default_args=default_args,
    catchup=False,
    schedule="*/5 * * * *",
) as dag:
    check_file_changed = DockerOperator(
        task_id="check_file_changed",
        image="ghcr.io/opendatabs/data-processing/stata_harvester:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment={
            **COMMON_ENV_VARS,
            "FTP_USER_01": Variable.get("FTP_USER_01"),
            "FTP_PASS_01": Variable.get("FTP_PASS_01"),
        },
        container_name="stata_harvester",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        do_xcom_push=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/harvesters/StatA/ftp-csv",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/stata_harvester/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )

    gate = ShortCircuitOperator(
        task_id="gate",
        python_callable=should_continue,
        provide_context=True,
    )

    ods_harvest = DockerOperator(
        task_id="ods_harvest",
        image="ghcr.io/opendatabs/data-processing/ods_harvest:latest",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl stata-ftp-csv",
        private_environment=COMMON_ENV_VARS,
        container_name="stata_harvester--ods_harvest",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    check_file_changed >> gate >> ods_harvest
