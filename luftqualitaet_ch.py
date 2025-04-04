"""
# luftqualitaet_ch
This DAG updates the following datasets:

- [100048](https://data.bs.ch/explore/dataset/100048)
- [100049](https://data.bs.ch/explore/dataset/100049)
- [100050](https://data.bs.ch/explore/dataset/100050)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the luftqualitaet_ch docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 29),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('luftqualitaet_ch', default_args=default_args, schedule_interval="15 * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='luftqualitaet_ch:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m luftqualitaet_ch.src.etl',
        container_name='luftqualitaet_ch',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
