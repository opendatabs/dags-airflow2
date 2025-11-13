"""
# dcc_dataspot_connector_fd_itbs_kdm
This DAG runs the Dataspot connector for the FD IT-BS KDM database.

- Connects to StatA test database
- Executes data extraction using Dataspot connector

***Note***
This is an Oracle database, and we had some trouble setting this up.
Here is the current settings (in german, since we will communicate in german
with other DB Admins aswell):

Unser Benutzer besitzt SELECT_CATALOG_ROLE Rechte auf alle KDM-Schemas.

Wir hatten das Problem, dass der Connector nach all_* tables gesucht hatte,
die Tabellen aber dba_* heissen. Darum wurden sie nicht gefunden (bzw. nur leere):

dba_*   => jeweils alle Objekte der DB
all_*      => jeweils alle Objekte, die dem eingeloggten User gehören oder die ihm gegranted worden sind
mit * = objects, views, constraints, tables etc.
In den Logfiles ersichtlich ist, dass Queries wie diese ausgeführt werden:
SELECT NULL AS pktable_cat,
       p.owner as pktable_schem,
       p.table_name as pktable_name
      FROM all_cons_columns pc, all_constraints p,
      all_cons_columns fc, all_constraints f
WHERE 1 = 1
  AND f.constraint_type = 'R'
…..
;
=>	Damit konnten eben keine Tabellen oder Spalten, auf die NICHT «select on » gegranted worden war
gesehen werden – also gar nichts.


*Lösung*:
Die Queries des Dataspots, oben ein Beispiel müssen angepasst werden von z.B.

SELECT NULL AS pktable_cat,
…
FROM all_cons_columns pc, all_constraints p,

Nach

SELECT NULL AS pktable_cat,
…
FROM dba_cons_columns pc, dba_constraints p,


*Hack*:
Wir gaukeln FD_ITBS_KDM_DATASPOT vor, er lese all_cons_columns, wenn er dba_cons_columns liest:

create or replace synonym FD_ITBS_KDM_DATASPOT.all_tab_columns for dba_tab_columns;

das wird eben mit anderen Views dann auch noch gemacht:
create or replace synonym FD_ITBS_KDM_DATASPOT.all_objects for dba_objects;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_tables for dba_tables;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_tab_cols for dba_tab_cols;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_users for dba_users;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_constraints for dba_constraints;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_cons_columns for dba_cons_columns;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_tab_comments  for dba_tab_comments;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_types for dba_types;
create or replace synonym FD_ITBS_KDM_DATASPOT.all_col_comments for dba_col_comments;

=>	Also für den User werden die all_*-views mit den dba_*-views «überschrieben» - dadurch
müssen die Queries vom dataspot connector nicht neu geschrieben werden.

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

CONTAINER_NAME = "dcc_dataspot_connector_fd_itbs_kdm"

# Configuration constants local
EXECUTABLE_CONNECTOR_JAR_FILE = "dataspot-connector-2025.1.3.jar"
SHARED_FOLDER_IN_DATA_EXCH = "FD-ITBS-KDM"
SERVICE_FILE_NAME = "myservice.yaml"
SERVICE_NAME = "KDMDatabaseService"

# Configuration constants on github
WORKDIR_FOLDER_IN_GITHUB = "fd-itsm-kdm"
APPLICATION_FILE_NAME = "application.yaml"

default_args = {
    "owner": "Renato Farruggio",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 27),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    CONTAINER_NAME,
    default_args=default_args,
    description=f"Run {CONTAINER_NAME}",
    schedule=None,  # TODO: Enable schedule when ready: "0 4 * * *"  # Run daily at 4 AM
    catchup=False,
) as dag:
    
    run_connector = DockerOperator(
        task_id=f"run_{CONTAINER_NAME}",
        image="eclipse-temurin:17-jre-alpine",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command=f"java -jar /opt/executable/{EXECUTABLE_CONNECTOR_JAR_FILE} --service={SERVICE_NAME} --file=/opt/configs/{SERVICE_FILE_NAME}",
        private_environment=COMMON_ENV_VARS,
        container_name=CONTAINER_NAME,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/dags-airflow2/dataspot-connector/{WORKDIR_FOLDER_IN_GITHUB}",
                target="/opt/workdir",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Executable",
                target="/opt/executable",
                type="bind",
            ),
            Mount(
                source="/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Driver",
                target="/opt/driver",
                type="bind",
            ),
            Mount(
                source=f"/mnt/OGD-DataExch/DCC/Dataspot/DatabaseConnector/Configurations/{SHARED_FOLDER_IN_DATA_EXCH}",
                target="/opt/configs",
                type="bind",
            ),
        ],
        working_dir="/opt/workdir",  # Set working directory in container
    )
