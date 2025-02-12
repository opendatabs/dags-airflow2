"""
# mobilitaet_mikromobilitaet_stats
This DAG updates the following datasets:

- [100416](https://data.bs.ch/explore/dataset/100416)
- [100418](https://data.bs.ch/explore/dataset/100418)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'orhan.saeedi',
    'description': 'Run the mobilitaet_mikromobilitaet_stats docker container',
    'depend_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG('mobilitaet_mikromobilitaet_stats', default_args=default_args, schedule_interval="0 4 * * *", catchup=False) as dag:
    dag.doc_md = __doc__
    process_upload = DockerOperator(
        task_id='process-upload',
        image='mobilitaet_mikromobilitaet_stats:latest',
        api_version='auto',
        auto_remove='force',
        command='python3 -m mobilitaet_mikromobilitaet.src.etl_stats',
        container_name='mobilitaet_mikromobilitaet_stats',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source="/data/dev/workspace/data-processing", target="/code/data-processing", type="bind"),
                Mount(source="/mnt/OGD-DataExch/StatA/BVD-MOB/Mikromobilitaet",
                      target="/code/data-processing/mobilitaet_mikromobilitaet/data", type="bind")
                ]
    )
