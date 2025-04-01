"""
# kapo_geschwindigkeitsmonitoring
This DAG updates the following datasets:

- [100112](https://data.bs.ch/explore/dataset/100112)
- [100115](https://data.bs.ch/explore/dataset/100115)
- [100097](https://data.bs.ch/explore/dataset/100097)
- [100200](https://data.bs.ch/explore/dataset/100200)
- [100358](https://data.bs.ch/explore/dataset/100358)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the kapo_geschwindigkeitsmonitoring docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('kapo_geschwindigkeitsmonitoring', default_args=default_args, schedule_interval='0 2 * * *',
         catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/data-processing/kapo_geschwindigkeitsmonitoring:latest',
        api_version='auto',
        auto_remove='force',
        command='uv run -m src.etl',
        container_name='kapo_geschwindigkeitsmonitoring',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring",
                      target="/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig", type="bind")]
    )

    rsync = DockerOperator(
        task_id='rsync',
        image='rsync:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m rsync.sync_files kapo_geschwindigkeitsmonitoring.json',
        container_name='kapo_geschwindigkeitsmonitoring--rsync',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/home/syncuser/.ssh/id_rsa", target="/root/.ssh/id_rsa", type="bind"),
                Mount(source="/data/dev/workspace", target="/code", type="bind")]
    )

    upload >> rsync
