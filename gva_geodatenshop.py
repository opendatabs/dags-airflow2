from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the gva-geodatenshop docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('gva_geodatenshop', default_args=default_args, schedule_interval="0 5 * * *", catchup=False) as dag:
    process_upload = DockerOperator(
        task_id='process-upload',
        image='gva_geodatenshop:latest',
        api_version='auto',
        auto_remove='force',
        command='/bin/bash /code/data-processing/gva_geodatenshop/etl.sh ',
        container_name='gva_geodatenshop',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-GVA", target="/code/data-processing/gva_geodatenshop/data_orig", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/harvesters/GVA",
                      target="/code/data-processing/gva_geodatenshop/data_harvester", type="bind")]
    )

    ods_harvest = DockerOperator(
        task_id='ods-harvest',
        image='ods_harvest:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_harvest.etl gva-ftp-csv',
        container_name='gva-geodatenshop--ods_harvest',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

    process_upload >> ods_harvest
