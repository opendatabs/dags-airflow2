# dags-airflow2

Repository to version the DAGs used in Airflow2 in pvpdstata02.

## Related Repositories

This repository is part of the [opendatabs](https://github.com/opendatabs) organization. For more information about the organization and its projects, see the [opendatabs organization README](https://github.com/opendatabs).

Most of the Docker images referenced in these DAGs are built and maintained in the [data-processing](https://github.com/opendatabs/data-processing) repository. For details about how these images are created, their structure, and how to contribute, refer to the [data-processing README](https://github.com/opendatabs/data-processing/blob/master/README.md).

## Overview

This repository contains Apache Airflow 2.7.2 DAG definitions that orchestrate data processing workflows. The DAGs primarily use Docker containers to execute ETL (Extract, Transform, Load) processes, with most containers hosted at `ghcr.io/opendatabs/data-processing/`.

## DAG Types

### Open Data DAGs

Most DAGs in this repository are for **Open Data** workflows. These DAGs:

- Process and publish datasets to the Open Data Portal (data.bs.ch)
- **Must include dataset IDs in the docstring** at the top of the file (visible in Airflow UI)
- Follow the **naming convention** that matches the folder name in the [data-processing](https://github.com/opendatabs/data-processing) repository
- Use Docker images from `ghcr.io/opendatabs/data-processing/{dag_id}:latest`

#### Dataset IDs in Docstrings

Open Data DAGs must document which datasets they affect in the docstring. This information is displayed in the Airflow UI and helps identify affected datasets when errors occur.

Example:

```python
"""
# aue_umweltlabor
This DAG updates the following datasets:

- [100066](https://data.bs.ch/explore/dataset/100066)
- [100067](https://data.bs.ch/explore/dataset/100067)
- [100068](https://data.bs.ch/explore/dataset/100068)
- [100069](https://data.bs.ch/explore/dataset/100069)
"""
```

The docstring is made visible in the Airflow UI by setting `dag.doc_md = __doc__` in the DAG definition.

#### Naming Convention

**Important**: The DAG `dag_id` must match the folder name in the [data-processing](https://github.com/opendatabs/data-processing) repository. This ensures:

- Consistent naming across repositories
- Correct Docker image references (`ghcr.io/opendatabs/data-processing/{dag_id}:latest`)
- Proper mount paths (`{PATH_TO_CODE}/data-processing/{dag_id}/...`)

For example:

- DAG file: `aue_umweltlabor.py` with `dag_id="aue_umweltlabor"`
- Corresponding folder in data-processing: `data-processing/aue_umweltlabor/`
- Docker image: `ghcr.io/opendatabs/data-processing/aue_umweltlabor:latest`

### Data Catalog DAGs (dcc_)

DAGs starting with `dcc_dataspot` are mostly for the **Data Catalog** (Dataspot), not Open Data. These DAGs:

- Sync metadata and organizational structures to the data catalog
- May use different Docker image registries (e.g., `ghcr.io/dcc-bs/dataspot:latest`)
- Do not publish to the Open Data Portal
- Examples: `dcc_dataspot_daily_jobs`, `dcc_dataspot_connector_*`, `dcc_dataspot_catalog_quality_daily`

### Special Cases: Other Repositories

Some DAGs use Docker images from repositories other than `data-processing`:

- **`rsync`**: Used for syncing files to remote servers
  - Image: `ghcr.io/opendatabs/rsync:latest`
  - Used in: `stata_konoer`, `stata_tourismusdashboard`, `kapo_smileys`, `kapo_geschwindigkeitsmonitoring`, etc.

- **`stata_konoer`**: R-based data processing
  - Image: `ghcr.io/opendatabs/stata_konoer:latest`
  - Used in: `stata_konoer.py`

- **`tourismusdashboard`**: Tourism dashboard data processing
  - Image: `ghcr.io/opendatabs/tourismusdashboard:latest`
  - Used in: `stata_tourismusdashboard.py`

These special cases are documented in the individual DAG files.

## Helpers

### FailureTrackingDockerOperator

Located in `helpers/failure_tracking_operator.py`, this custom operator extends the standard `DockerOperator` to include failure tracking capabilities.

#### When to Use

Use `FailureTrackingDockerOperator` when you want to tolerate a certain number of consecutive failures before actually failing a task. This is useful for:

- Intermittent data source issues
- Network connectivity problems
- Temporary API outages
- Frequently scheduled DAGs where occasional failures are expected
- Any scenario where occasional failures are expected but shouldn't immediately fail the DAG

#### How to Use

The typical pattern is to define configuration constants at the top of your DAG file:

```python
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from docker.types import Mount
from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

# DAG configuration
DAG_ID = "my_dag"
FAILURE_THRESHOLD = 1  # Skip first failure, fail on second
EXECUTION_TIMEOUT = timedelta(minutes=3)
SCHEDULE = "*/5 * * * *"

default_args = {
    "owner": "your.name",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    upload = FailureTrackingDockerOperator(
        task_id="upload",
        failure_threshold=FAILURE_THRESHOLD,
        execution_timeout=EXECUTION_TIMEOUT,
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
```

#### Parameters

- **`failure_threshold`** (int, required): Number of consecutive failures to tolerate before actually failing the task
  - `0` = immediate failure with no skipping (same as regular DockerOperator)
  - `5` = skip first 5 failures, fail on the 6th consecutive failure
  - Common values in the codebase: 1, 2, 4, 5, 16 (depending on DAG frequency and reliability requirements)

- **`execution_timeout`** (timedelta or None, required): Maximum time allowed for task execution. Task fails if exceeded.
  - Use `timedelta(minutes=30)` for a 30-minute timeout
  - Use `timedelta(seconds=50)` for a 50-second timeout
  - Use `None` for no timeout (not recommended)
  - Common values in the codebase: `timedelta(minutes=2)` to `timedelta(minutes=90)`

**Note**: Both `failure_threshold` and `execution_timeout` are required parameters and must be provided when using `FailureTrackingDockerOperator`. All other parameters are the same as `DockerOperator`.

#### How It Works

The operator tracks consecutive failures using Airflow Variables (stored as `{dag_id}_{task_id}_consecutive_failures`).

- On each failure, the count increments
- If the count exceeds the threshold, the task fails and raises an exception
- If the count is under the threshold, the task is skipped (raises `AirflowSkipException`)
- On success, the counter resets to 0

This allows DAGs to continue running even when individual task executions fail, as long as the failure count stays below the threshold. This is particularly useful for frequently scheduled DAGs where occasional failures are expected.

## Docker Container Cleanup

Some DAGs include a cleanup task at the beginning to remove any old containers that may have been left behind from previous runs. This prevents container name conflicts and ensures clean execution.

### When to Use

Add a cleanup task when:

- Your DAG uses a fixed `container_name` (not auto-generated)
- Previous failed runs might have left containers running
- You're experiencing container name conflicts

### How to Implement

```python
from airflow.operators.bash import BashOperator

# Cleanup task to remove any old containers at the beginning
cleanup_containers = BashOperator(
    task_id="cleanup_old_containers",
    bash_command=f'''
        docker rm -f {DAG_ID} 2>/dev/null || true
        ''',
)

# Set the task dependency
cleanup_containers >> your_main_task
```

The `2>/dev/null || true` ensures the command doesn't fail if the container doesn't exist.

## Common Variables

The `common_variables.py` module provides shared configuration:

- **`COMMON_ENV_VARS`**: Dictionary of common environment variables (proxy settings, email configuration, FTP credentials, ODS API keys)
- **`PATH_TO_CODE`**: Path to the code directory (set via Airflow Variable)

These are typically used via:

```python
from common_variables import COMMON_ENV_VARS, PATH_TO_CODE
```

## DAG Configuration Fields

### DAG Object

The `DAG` object is the main container for your workflow. Reference: [Airflow 2.7.2 DAG Documentation](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html)

#### Common Fields

- **`dag_id`** (str, required): Unique identifier for the DAG. Must be unique across all DAGs.
  - Reference: [DAG ID](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#dag-id)

- **`description`** (str, optional): Description of what the DAG does. Displayed in the Airflow UI.
  - Reference: [DAG Description](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#dag-description)

- **`default_args`** (dict, optional): Default arguments applied to all tasks in the DAG. Can be overridden at the task level.
  - Reference: [Default Arguments](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#default-arguments)

- **`schedule`** (str, timedelta, or None, optional): Schedule for the DAG execution.
  - Cron expression: `"0 6 * * *"` (daily at 6 AM)
  - `None` for manually triggered DAGs
  - Reference: [Scheduling](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#scheduling)

- **`catchup`** (bool, optional): Whether to backfill missed DAG runs. Set to `False` to prevent catchup.
  - Reference: [Catchup](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#catchup)

### default_args Dictionary

Common fields used in `default_args`:

- **`owner`** (str): Owner of the DAG (typically an email or username)
  - Reference: [Task Owner](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#owner)

- **`depends_on_past`** (bool): Whether a task depends on the success of the previous task instance
  - Reference: [depends_on_past](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#depends-on-past)

- **`start_date`** (datetime): The first execution date for the DAG
  - Reference: [Start Date](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/dags.html#start-date)

- **`email`** (str or list): Email address(es) to send notifications to
  - Reference: [Email Notifications](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/notifications.html)

- **`email_on_failure`** (bool): Send email on task failure
  - Reference: [Email on Failure](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/notifications.html#email-on-failure)

- **`email_on_retry`** (bool): Send email on task retry
  - Reference: [Email on Retry](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/notifications.html#email-on-retry)

- **`retries`** (int): Number of retries for failed tasks
  - Reference: [Retries](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#retries)

- **`retry_delay`** (timedelta): Delay between retries
  - Reference: [Retry Delay](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#retry-delay)

## DockerOperator Fields

The `DockerOperator` runs Docker containers as Airflow tasks. Reference: [Docker Operator Documentation](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html)

### Common DockerOperator Fields

- **`task_id`** (str, required): Unique identifier for the task within the DAG
  - Reference: [Task ID](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#task-id)

- **`image`** (str, required): Docker image to use (e.g., `"ghcr.io/opendatabs/data-processing/my_dag:latest"`)
  - Reference: [Docker Image](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#image)

- **`command`** (str or list, optional): Command to run in the container
  - Can be a string: `"uv run -m etl"`
  - Or a list: `["java", "-jar", "/app/app.jar"]`
  - Reference: [Command](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#command)

- **`container_name`** (str, optional): Name for the Docker container. If not provided, a name is auto-generated.
  - Reference: [Container Name](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#container-name)

- **`force_pull`** (bool, optional): Whether to pull the image even if it already exists locally. Set to `True` to always get the latest version.
  - Reference: [Force Pull](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#force-pull)

- **`api_version`** (str, optional): Docker API version. Use `"auto"` to auto-detect.
  - Reference: [API Version](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#api-version)

- **`auto_remove`** (str, optional): Whether to remove the container after execution.
  - `"force"` = always remove, even on failure
  - `True` = remove on success
  - `False` = never remove
  - Reference: [Auto Remove](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#auto-remove)

- **`mount_tmp_dir`** (bool, optional): Whether to mount a temporary directory. Set to `False` to disable.
  - Reference: [Mount Tmp Dir](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#mount-tmp-dir)

- **`docker_url`** (str, optional): URL to the Docker daemon. Use `"unix://var/run/docker.sock"` for local Docker.
  - Reference: [Docker URL](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#docker-url)

- **`network_mode`** (str, optional): Docker network mode (e.g., `"bridge"`, `"host"`).
  - Reference: [Network Mode](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#network-mode)

- **`tty`** (bool, optional): Whether to allocate a pseudo-TTY. Set to `True` for better log output.
  - Reference: [TTY](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#tty)

- **`private_environment`** (dict, optional): Environment variables to pass to the container (sensitive values are hidden in logs).
  - Reference: [Private Environment](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#private-environment)

- **`environment`** (dict, optional): Environment variables to pass to the container (visible in logs).
  - Reference: [Environment](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#environment)

- **`mounts`** (list, optional): List of `docker.types.Mount` objects to mount volumes into the container.
  - Example:

    ```python
    from docker.types import Mount
    
    mounts=[
        Mount(
            source="/host/path",
            target="/container/path",
            type="bind",
        ),
    ]
    ```

  - Reference: [Mounts](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#mounts)

- **`working_dir`** (str, optional): Working directory inside the container.
  - Reference: [Working Directory](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/operators/docker.html#working-dir)

- **`execution_timeout`** (timedelta, optional): Maximum time allowed for task execution. Task fails if exceeded.
  - Reference: [Execution Timeout](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/tasks.html#execution-timeout)

## Example DAG Structure

### Open Data DAG Example

```python
"""
# my_dag
This DAG updates the following datasets:

- [100001](https://data.bs.ch/explore/dataset/100001)
- [100002](https://data.bs.ch/explore/dataset/100002)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

DAG_ID = "my_dag"  # Must match folder name in data-processing repository

default_args = {
    "owner": "your.name",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Run the {DAG_ID} docker container",
    schedule="0 6 * * *",  # Daily at 6 AM
    catchup=False,
) as dag:
    dag.doc_md = __doc__  # Makes the docstring visible in Airflow UI
    
    process_task = DockerOperator(
        task_id="process",
        image=f"ghcr.io/opendatabs/data-processing/{DAG_ID}:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="uv run -m etl",
        private_environment=COMMON_ENV_VARS,
        container_name=DAG_ID,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
        mounts=[
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/change_tracking",
                target="/code/change_tracking",
                type="bind",
            ),
        ],
    )
```

### Data Catalog DAG Example (dcc_)

```python
"""
# dcc_dataspot_sync_example
This DAG syncs data catalog metadata.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from common_variables import COMMON_ENV_VARS

default_args = {
    "owner": "your.name",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dcc_dataspot_sync_example",
    default_args=default_args,
    description="Sync data catalog metadata",
    schedule="0 3 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
    sync_task = DockerOperator(
        task_id="sync",
        image="ghcr.io/dcc-bs/dataspot:latest",
        force_pull=True,
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
        command="python -m scripts.sync_example",
        private_environment=COMMON_ENV_VARS,
        container_name="dcc_dataspot_sync_example",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        tty=True,
    )
```

## Additional Resources

- [Apache Airflow 2.7.2 Documentation](https://airflow.apache.org/docs/apache-airflow/2.7.2/)
- [Docker Provider for Airflow Documentation](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.3.1/)
- [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/2.7.2/concepts/index.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/2.7.2/best-practices.html)
