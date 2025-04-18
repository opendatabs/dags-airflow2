"""
# aue_rues
This DAG updates the following datasets:

- [100046](https://data.bs.ch/explore/dataset/100046)
- [100323](https://data.bs.ch/explore/dataset/100323)
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# This is set in the Airflow UI under Admin -> Variables
PATH_TO_CODE = Variable.get("PATH_TO_CODE")
# For common
https_proxy = Variable.get("https_proxy")
http_proxy = Variable.get("http_proxy")
EMAIL_RECEIVERS = Variable.get("EMAIL_RECEIVERS")
EMAIL_SERVER = Variable.get("EMAIL_SERVER")
EMAIL = Variable.get("EMAIL")
FTP_SERVER = Variable.get("FTP_SERVER")
FTP_USER = Variable.get("FTP_USER")
FTP_PASS = Variable.get("FTP_PASS")
ODS_API_KEY = Variable.get("ODS_API_KEY")
# For etl job
FTP_USER_03 = Variable.get("FTP_USER_03")
FTP_PASS_03 = Variable.get("FTP_PASS_03")
ODS_PUSH_URL_100046 = Variable.get("ODS_PUSH_URL_100046")
ODS_PUSH_URL_100323 = Variable.get("ODS_PUSH_URL_100323")

default_args = {
    'owner': 'jonas.bieri',
    'description': 'Run the aue_rues docker container',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 19),
    'email': ["jonas.bieri@bs.ch", "orhan.saeedi@bs.ch", "rstam.aloush@bs.ch", "renato.farruggio@bs.ch"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG('aue_rues', default_args=default_args, schedule_interval="*/10 * * * *", catchup=False,
         dagrun_timeout=timedelta(minutes=8)) as dag:
    dag.doc_md = __doc__
    upload_bag_datasets = DockerOperator(
        task_id='upload',
        image='ghcr.io/opendatabs/data-processing/aue_rues:latest',
        force_pull=True,
        api_version='auto',
        auto_remove='force',
        command='uv run -m etl',
        environment={
            'https_proxy': https_proxy,
            'http_proxy': http_proxy,
            'EMAIL_RECEIVERS': EMAIL_RECEIVERS,
            'EMAIL_SERVER': EMAIL_SERVER,
            'EMAIL': EMAIL,
            'FTP_SERVER': FTP_SERVER,
            'FTP_USER': FTP_USER,
            'FTP_PASS': FTP_PASS,
            'ODS_API_KEY': ODS_API_KEY,
            'FTP_USER_03': FTP_USER_03,
            'FTP_PASS_03': FTP_PASS_03,
            'ODS_PUSH_URL_100046': ODS_PUSH_URL_100046,
            'ODS_PUSH_URL_100323': ODS_PUSH_URL_100323
        },
        container_name='aue_rues--upload',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[Mount(source=f"{PATH_TO_CODE}/data-processing/aue_rues/data_orig",
                      target="/code/data_orig", type="bind")]
    )
