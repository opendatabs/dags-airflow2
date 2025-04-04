"""
# meteoblue_wolf
This DAG updates the following datasets:

- [100009](https://data.bs.ch/explore/dataset/100009)
- [100082](https://data.bs.ch/explore/dataset/100082)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the meteoblue-wolf docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 22),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('meteoblue_wolf', default_args=default_args, schedule_interval="10 * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id='process-upload',
        image='meteoblue_wolf:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m meteoblue_wolf.etl',
        container_name='meteoblue_wolf',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

    ods_publish = DockerOperator(
        task_id='ods-publish',
        image='ods_publish:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_publish.etl_id 100009,100082',
        container_name='meteoblue-wolf--ods_publish',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

    process_upload >> ods_publish
