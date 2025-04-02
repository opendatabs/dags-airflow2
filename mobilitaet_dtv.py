"""
# mobilitaet_dtv.py
This DAG updates the following datasets:

- [100199](https://data.bs.ch/explore/dataset/100199)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the mobilitaet_dtv docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('mobilitaet_dtv', default_args=default_args, schedule_interval="0 3 * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/data-processing/mobilitaet_dtv:latest',
        api_version='auto',
        auto_remove='force',
        command='uv run -m etl',
        container_name='mobilitaet_dtv--upload',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
