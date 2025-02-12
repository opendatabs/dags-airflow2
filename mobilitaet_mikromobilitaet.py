"""
# mobilitaet_mikromobilitaet
This DAG updates the following datasets:

- [100413](https://data.bs.ch/explore/dataset/100415)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'orhan.saeedi',
    'description': 'Run the mobilitaet_mikromobilitaet docker container',
    'depend_on_past': False,
    'start_date': datetime(2025, 1, 31),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('mobilitaet_mikromobilitaet', default_args=default_args, schedule_interval="*/10 * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id='process-upload',
        image='mobilitaet_mikromobilitaet:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m mobilitaet_mikromobilitaet.src.etl',
        container_name='mobilitaet_mikromobilitaet',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/BVD-MOB/Mikromobilitaet",
                      target="/code/data-processing/mobilitaet_mikromobilitaet/data", type="bind")
                ]
    )
