"""
# covid19_dashboard
This DAG updates the following datasets:

- [100085](https://data.bs.ch/explore/dataset/100085)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the covid19dashboard docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 26),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('covid19_dashboard', default_args=default_args, schedule_interval="0 * * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    upload = DockerOperator(
        task_id='upload',
        image='covid19dashboard:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m covid19dashboard.etl',
        container_name='covid19dashboard',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind")]
    )

    # ods_publish = DockerOperator(
    #         task_id='ods-publish',
    #         image='ods_publish:latest',
    #         api_version='auto',
    #         auto_remove=True,
    #         command='python3 -m ods_publish.etl_id 100085',
    #         container_name='covid19dashboard--ods_publish',
    #         docker_url="unix://var/run/docker.sock",
    #         network_mode="bridge",
    #         tty=True,
    #         volumes=['/data/dev/workspace/data-processing:/code/data-processing'],
    #         retry=5,
    #         retry_delay=timedelta(minutes=5)
    # )
    #
    # upload >> ods_publish
