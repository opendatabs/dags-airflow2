"""
# ods_catalog
This DAG updates the following datasets:

- [100057](https://data.bs.ch/explore/dataset/100057)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the ods_catalog docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}


with DAG('ods_catalog', default_args=default_args, schedule_interval='3 * * * *', catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='ods_catalog:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_catalog.etl',
        container_name='ods_catalog',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/FST-OGD",
                      target="/code/data-processing/ods_catalog/data", type="bind")]
    )
