"""
# bafu_hydro_daten
This DAG updates the following dataset:

- [100294](https://data.bs.ch/explore/dataset/100294)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# This is set in the Airflow UI under Admin -> Variables
https_proxy = Variable.get("https_proxy")

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
        image='ghcr.io/opendatabs/data-processing/meteoblue_rosental:latest',
        force_pull=True,
        api_version='auto',
        auto_remove='force',
        environment={'https_proxy': https_proxy},
        command='uv run -m etl',
        container_name='meteoblue_rosental',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
