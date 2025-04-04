"""
# aue-umweltlabor
This DAG updates the following datasets:

- [100066](https://data.bs.ch/explore/dataset/100066)
- [100067](https://data.bs.ch/explore/dataset/100067)
- [100068](https://data.bs.ch/explore/dataset/100068)
- [100069](https://data.bs.ch/explore/dataset/100069)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the aue-umweltlabor docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 19),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30)
}

with DAG('aue_umweltlabor', default_args=default_args, schedule_interval="0 6 * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id='process-upload',
        image='aue_umweltlabor:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m aue_umweltlabor.etl',
        container_name='aue_umweltlabor',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/Umweltlabor", target="/code/data-processing/aue_umweltlabor/data_orig",
                      type="bind")]
    )

    ods_publish = DockerOperator(
        task_id='ods-publish',
        image='ods_publish:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m ods_publish.etl_id 100066,100067,100068',
        container_name='aue_umweltlabor--ods_publish',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

    process_upload >> ods_publish
