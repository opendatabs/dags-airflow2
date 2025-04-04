"""
# fgi_geodatenshop
This DAG updates the following datasets:

- [100395](https://data.bs.ch/explore/dataset/100395)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'rstam.aloush',
    'description': 'Run the fgi_geodatenshop docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 9, 25),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}


with DAG('fgi_geodatenshop', default_args=default_args, schedule_interval='0 */2 * * *', catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='fgi_geodatenshop:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m fgi_geodatenshop.etl',
        container_name='fgi_geodatenshop',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/harvesters/FGI",
                      target="/code/data-processing/fgi_geodatenshop/data", type="bind")]
    )

    ods_harvest = DockerOperator(
        task_id='ods-harvest',
        image='ods_harvest:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_harvest.etl gva-gpkg-ftp-csv',
        container_name='fgi_geodatenshop--ods_harvest',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

upload >> ods_harvest
