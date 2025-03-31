"""
# staka_regierungsratsbeschluesse.py
This DAG updates the following datasets:

- [100427](https://data.bs.ch/explore/dataset/100427)
"""

import time
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")

default_args = {
    'owner': 'orhan.saeedi',
    'description': 'Run the staka_regierungsratsbeschluesse docker container',
    'depend_on_past': False,
    'start_date': datetime(2025, 2, 26),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('staka_regierungsratsbeschluesse', default_args=default_args, schedule_interval='*/5 * * * 2,3',
         catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='staka_regierungsratsbeschluesse:latest',
        api_version='auto',
        auto_remove='force',
        environment={'https_proxy': https_proxy},
        command='uv run -m src.etl',
        container_name='staka_regierungsratsbeschluesse',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
