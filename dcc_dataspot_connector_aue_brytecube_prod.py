"""
# dcc_dataspot_connector_stata_ad_test
This DAG runs the Dataspot connector for StatA test database - with AD authentication.

- Connects to StatA test database using AD authentication (not Azure!)
- Executes data extraction using Dataspot connector
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

CONTAINER_NAME = "dcc_dataspot_connector_aue_brytecube_prod"

# Configuration constants local
EXECUTABLE_CONNECTOR_JAR_FILE = "dataspot-connector-2025.1.3.jar"
SHARED_FOLDER_IN_DATA_EXCH = "AUE-BryteCube-Prod"
SERVICE_FILE_NAME = "myservice.yaml"
SERVICE_NAME = "MyDatabaseService"

# Configuration constants on github
WORKDIR_FOLDER_IN_GITHUB = "aue-brytecube-prod" # Also used for Docker image name
APPLICATION_FILE_NAME = "application.yaml"

default_args = {
    "owner": "renato.farruggio",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 27),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    CONTAINER_NAME,
    default_args=default_args,
    description=f"Run {CONTAINER_NAME}",
    schedule=None,  # TODO: Enable schedule when ready: "0 4 * * *"  # Run daily at 4 AM
    catchup=False,
) as dag:
    
    run_connector = DockerOperator(
        task_id=f"run_{CONTAINER_NAME}",
        image=f"ghcr.io/dcc-bs/dataspot/connectors/{WORKDIR_FOLDER_IN_GITHUB}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command=[
            "java",
            "-Djava.security.auth.login.config=/tmp/jaas.conf",
            "-Djava.security.krb5.conf=/etc/krb5.conf",
            "-Djavax.security.auth.useSubjectCredsOnly=false",
            "-jar",
            f"/opt/executable/{EXECUTABLE_CONNECTOR_JAR_FILE}",
            f"--service={SERVICE_NAME}",
            f"--file=/opt/configs/{SERVICE_FILE_NAME}",
        ],
        private_environment={
            **COMMON_ENV_VARS,
            'AD_USERNAME': Variable.get("AUE_AD_USERNAME"),
            'AD_PASSWORD': Variable.get("AUE_AD_PASSWORD"),
            'AD_DOMAIN_CONTROLLER': Variable.get("AD_DOMAIN_CONTROLLER"),
            'AD_REALM': Variable.get("AD_REALM"),
        },
        container_name=CONTAINER_NAME,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/dags-airflow2/dataspot-connector/{WORKDIR_FOLDER_IN_GITHUB}",
                target="/opt/workdir",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Executable",
                target="/opt/executable",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Driver",
                target="/opt/driver",
                type="bind",
            ),
            Mount(
                source=f"/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Configurations/{SHARED_FOLDER_IN_DATA_EXCH}",
                target="/opt/configs",
                type="bind",
            ),
        ],
        working_dir="/opt/workdir",  # Set working directory in container
    )