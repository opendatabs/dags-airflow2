"""
# aue_grundwasser
This DAG updates the following datasets:

- [100164](https://data.bs.ch/explore/dataset/100164)
- [100179](https://data.bs.ch/explore/dataset/100179)
- [100180](https://data.bs.ch/explore/dataset/100180)
- [100181](https://data.bs.ch/explore/dataset/100181)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the aue_grundwasser docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 17),
    'email': ["jonas.bieri@bs.ch", "jonas.eckenfels@bs.ch", "orhan.saeedi@bs.ch",
              "nicolas.maire@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('aue_grundwasser', default_args=default_args, schedule_interval="25 5 * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id='upload',
        image='aue_grundwasser:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m aue_grundwasser.etl',
        container_name='aue_grundwasser--upload',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )
