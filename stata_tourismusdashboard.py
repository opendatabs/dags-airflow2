"""
# stata_tourismusdashboard.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")
http_proxy = Variable.get("http_proxy")
PATH_TO_CODE = Variable.get("PATH_TO_CODE")
DB_CONNECTION_STRING_TOURISMUS = Variable.get("DB_CONNECTION_STRING_TOURISMUS")

default_args = {
    "owner": "orhan.saeedi",
    "depend_on_past": False,
    "start_date": datetime(2025, 4, 11),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "stata_tourismusdashboard",
    default_args=default_args,
    description="Run the stata_tourismusdashboard docker container",
    schedule_interval="*/15 * * * *",
    catchup=False,
) as dag:
    # Dummy operators for the "skip" paths
    skip_embargo_100413 = EmptyOperator(task_id="skip_embargo_100413")
    skip_embargo_100414 = EmptyOperator(task_id="skip_embargo_100414")

    embargo_100413 = DockerOperator(
        task_id="embargo_100413",
        image="python:3.12-slim",
        command="python3 /code/check_embargo.py /code/data/100413_tourismus-daily_embargo.txt",
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard",
                target="/code",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    embargo_100414 = DockerOperator(
        task_id="embargo_100414",
        image="python:3.12-slim",
        command="python3 /code/check_embargo.py /code/data/100414_tourismus-daily_embargo.txt",
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard",
                target="/code",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )

    write_to_DataExch = DockerOperator(
        task_id="write_to_DataExch",
        image="ghcr.io/opendatabs/tourismusdashboard:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="Rscript /code/app_write_OGD.R",
        private_environment={
            "https_proxy": https_proxy,
            "http_proxy": http_proxy,
            "DB_CONNECTION_STRING_TOURISMUS": DB_CONNECTION_STRING_TOURISMUS,
        },
        container_name="write_to_DataExch",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            )
        ],
    )

    load_to_DataExch = DockerOperator(
        task_id="load_to_DataExch",
        image="ghcr.io/opendatabs/tourismusdashboard:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="Rscript /code/app_load_from_OGD.R",
        private_environment={
            "https_proxy": https_proxy,
            "http_proxy": http_proxy,
        },
        container_name="load_to_DataExch",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            )
        ],
    )

    write_to_data = DockerOperator(
        task_id="write_to_data",
        image="ghcr.io/opendatabs/tourismusdashboard:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="Rscript /code/app_write_OGD.R",
        private_environment={
            "https_proxy": https_proxy,
            "http_proxy": http_proxy,
            "DB_CONNECTION_STRING_TOURISMUS": DB_CONNECTION_STRING_TOURISMUS,
        },
        container_name="write_to_data",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard/data",
                target="/code/data",
                type="bind",
            )
        ],
    )

    load_to_data = DockerOperator(
        task_id="load_to_data",
        image="ghcr.io/opendatabs/tourismusdashboard:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        command="Rscript /code/app_load_from_OGD.R",
        private_environment={
            "https_proxy": https_proxy,
            "http_proxy": http_proxy,
        },
        container_name="load_to_data",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard/data",
                target="/code/data",
                type="bind",
            )
        ],
    )

    rsync_test_1 = DockerOperator(
        task_id="rsync_test_1",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_test_1.json",
        container_name="rsync_test_1",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_test_2 = DockerOperator(
        task_id="rsync_test_2",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_test_2.json",
        container_name="rsync_test_2",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_prod_1 = DockerOperator(
        task_id="rsync_prod_1",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_prod_1.json",
        container_name="rsync_prod_1",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_prod_2 = DockerOperator(
        task_id="rsync_prod_2",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_prod_2.json",
        container_name="rsync_prod_2",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source="/mnt/OGD-DataExch/StatA/Tourismus",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_public_1 = DockerOperator(
        task_id="rsync_public_1",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_test_1.json",
        container_name="rsync_public_1",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard/data",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    rsync_public_2 = DockerOperator(
        task_id="rsync_public_2",
        image="rsync:latest",
        api_version="auto",
        auto_remove="force",
        command="python3 -m rsync.sync_files stata_tourismus_test_2.json",
        container_name="rsync_public_2",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source="/home/syncuser/.ssh/id_rsa",
                target="/root/.ssh/id_rsa",
                type="bind",
            ),
            Mount(source=PATH_TO_CODE, target="/code", type="bind"),
            Mount(
                source=f"{PATH_TO_CODE}/R-data-processing/tourismusdashboard/data",
                target="/code/data",
                type="bind",
            ),
        ],
    )

    (
        write_to_DataExch
        >> load_to_DataExch
        >> [rsync_test_1, rsync_test_2, rsync_prod_1, rsync_prod_2]
    )

    # Chain embargo tasks to write_to_data with conditional logic
    write_to_data.trigger_rule = (
        TriggerRule.ONE_SUCCESS
    )  # Either embargo task "proceeds" (non-skipped) triggers this

    # Embargo chain setup
    [embargo_100413, skip_embargo_100413] >> write_to_data
    [embargo_100414, skip_embargo_100414] >> write_to_data

    # Set downstream trigger rules for continued robustness
    load_to_data.trigger_rule = TriggerRule.ALL_DONE
    rsync_public_1.trigger_rule = TriggerRule.ALL_DONE
    rsync_public_2.trigger_rule = TriggerRule.ALL_DONE

    write_to_data >> load_to_data >> [rsync_public_1, rsync_public_2]
