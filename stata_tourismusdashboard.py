"""
# stata_tourismusdashboard.py
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")
http_proxy = Variable.get("http_proxy")
DB_DRIVER = Variable.get("DB_DRIVER")
DB_SERVER = Variable.get("DB_SERVER")
DB_NAME = Variable.get("DB_NAME_TOURISMUSDASHBOARD")
DB_VIEW = Variable.get("DB_VIEW_TOURISMUSDASHBOARD")
DB_UID = Variable.get("DB_UID")
DB_PWD = Variable.get("DB_PWD")


default_args = {
    'owner': 'orhan.saeedi',
    'description': 'Run the stata_tourismusdashboard docker container',
    'depend_on_past': False,
    'start_date': datetime(2025, 4, 11),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}
with DAG('stata_tourismusdashboard', default_args=default_args, schedule_interval="0 10 * * *", catchup=False) as dag:
    upload = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/tourismusdashboard:latest',
        force_pull=True,
        api_version='auto',
        auto_remove='force',
        command='Rscript /code/app_write_OGD.R',
        environment={
            'https_proxy': https_proxy,
            'http_proxy': http_proxy,
            'DB_DRIVER': DB_DRIVER,
            'DB_SERVER': DB_SERVER,
            'DB_NAME': DB_NAME,
            'DB_VIEW': DB_VIEW,
            'DB_UID': DB_UID,
            'DB_PWD': DB_PWD
        },
        container_name='stata_tourismusdashboard--transform',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/mnt/OGD-DataExch/StatA/Tourismus", target="/code/data", type="bind")]
    )

    upload
