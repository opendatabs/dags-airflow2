"""
# mobilitaet_verkehrszaehldaten
This DAG updates the following datasets:

- [100006](https://data.bs.ch/explore/dataset/100006)
- [100013](https://data.bs.ch/explore/dataset/100013)
- [100356](https://data.bs.ch/explore/dataset/100356)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the mobilitaet_verkehrszaehldaten docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('mobilitaet_verkehrszaehldaten', default_args=default_args, schedule_interval="0 6 * * *",
         catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='mobilitaet_verkehrszaehldaten:latest',
        api_version='auto',
        auto_remove='force',
        command='uv run -m src.etl',
        container_name='mobilitaet_verkehrszaehldaten',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/MOB-StatA", target="/code/data-processing/mobilitaet_verkehrszaehldaten/data_orig",
                      type="bind")]
    )

    ods_publish = DockerOperator(
        task_id='ods-publish',
        image='ods-publish:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_publish.etl_id 100006,100013,100356',
        container_name='mobilitaet_verkehrszaehldaten--ods-publish',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")],
    )

    rsync = DockerOperator(
        task_id='rsync',
        image='rsync:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m rsync.sync_files mobilitaet_verkehrszaehldaten.json',
        container_name='mobilitaet_verkehrszaehldaten--rsync',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/home/syncuser/.ssh/id_rsa", target="/root/.ssh/id_rsa", type="bind"),
                Mount(source="/data/dev/workspace", target="/code", type="bind")]
    )

    upload >> ods_publish
    upload >> rsync
