"""
# parkhaeuser
This DAG updates the following datasets:

- [100088](https://data.bs.ch/explore/dataset/100088)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'renato.farruggio',
    'description': 'Run the parkhaeuser docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 10, 12),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('parkhaeuser', default_args=default_args, schedule_interval="* * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id='process-upload',
        image='parkhaeuser:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m parkhaeuser.etl',
        container_name='parkhaeuser',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
