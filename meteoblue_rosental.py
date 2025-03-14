"""
# bafu_hydro_daten
This DAG updates the following dataset:

- [100294](https://data.bs.ch/explore/dataset/100294)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'hester.pieters',
    'description': 'Run the meteoblue_rosental docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 26),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('meteoblue_rosental', default_args=default_args, schedule_interval="45 * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='meteoblue_rosental:latest',
        api_version='auto',
        auto_remove='force',
        command='uv run -m etl',
        container_name='meteoblue_rosental',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
