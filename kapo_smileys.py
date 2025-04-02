"""
# kapo_smileys
This DAG updates the following datasets:

- [100268](https://data.bs.ch/explore/dataset/100268)
- [100277](https://data.bs.ch/explore/dataset/100277)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the kapo_smileys docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 22),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('kapo_smileys', default_args=default_args, schedule_interval="15 3 * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/data-processing/kapo_smileys:latest',
        api_version='auto',
        auto_remove='force',
        environment={'https_proxy': https_proxy},
        command='uv run -m src.etl',
        container_name='kapo_smileys--upload',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/kapo-smileys", target="/code/data-processing/kapo_smileys/data_orig",
                      type="bind")]
    )

    rsync = DockerOperator(
        task_id='rsync',
        image='rsync:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m rsync.sync_files kapo_smileys.json',
        container_name='kapo_smileys--rsync',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/home/syncuser/.ssh/id_rsa", target="/root/.ssh/id_rsa", type="bind"),
                Mount(source="/data/dev/workspace", target="/code", type="bind")]
    )

    upload >> rsync