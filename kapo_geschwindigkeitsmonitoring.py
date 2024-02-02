"""
# kapo_geschwindigkeitsmonitoring
This DAG updates the following datasets:

- [100112](https://data.bs.ch/explore/dataset/100112)
- [100115](https://data.bs.ch/explore/dataset/100115)
- [100097](https://data.bs.ch/explore/dataset/100097)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the kapo_geschwindigkeitsmonitoring docker container',
    'depend_on_past': False,
    'start_date': datetime(2021, 1, 20),
    'email': ["jonas.bieri@bs.ch", "jonas.eckenfels@bs.ch", "orhan.saeedi@bs.ch", "nicolas.maire@bs.ch"],
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
        image='kapo_geschwindigkeitsmonitoring:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m kapo_geschwindigkeitsmonitoring.etl',
        container_name='kapo_geschwindigkeitsmonitoring',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        volumes=[
            '/mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig',
            '/data/dev/workspace/data-processing:/code/data-processing']
    )
