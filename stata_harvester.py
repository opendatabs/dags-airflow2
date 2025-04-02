from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'orhan.saeedi',
    'description': 'Run the stata_harvester docker container',
    'depend_on_past': False,
    'start_date': datetime(2025, 2, 26),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('stata_harvester', default_args=default_args, catchup=False, schedule_interval="*/5 * * * *") as dag:
    ''' TODO: Implement a FileSensor that works
    poking = FileSensor(
        task_id='poking',
        filepath='/mnt/OGD-DataExch/StatA/harvesters/StatA/ftp-csv/OpendataSoft_Export_Stata.csv',
        poke_interval=10
    )
    '''

    upload = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/data-processing/stata_harvester:latest',
        api_version='auto',
        auto_remove='force',
        command='uv run -m etl',
        container_name='stata_harvester',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/harvesters/StatA/ftp-csv",
                        target="/code/data-processing/stata_harvester/data_orig", type="bind")]
    )
