"""
Utility module for handling failure tracking in DAGs.
"""

from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from typing import Dict, Any


def execute_docker_with_failure_tracking(
    dag_id: str,
    task_id: str,
    failure_threshold: int,
    docker_operator_kwargs: Dict[str, Any],
    **context
) -> Any:
    """
    Wrapper function that executes Docker container and handles failures with a threshold
    
    Parameters:
        dag_id: The ID of the DAG
        task_id: Task identifier
        failure_threshold: Number of consecutive failures allowed before actually failing
        docker_operator_kwargs: Arguments to pass to the DockerOperator
        **context: The Airflow task context (automatically provided by Airflow)
    
    Returns:
        Result from the Docker execution
    
    Raises:
        AirflowSkipException: If failure count is under threshold
        Exception: If failure count exceeds threshold
    """
    # Get failure variable name
    failure_var = f"{dag_id}_consecutive_failures"
    
    # Get failure count
    failure_count = int(Variable.get(failure_var, default_var=0))
    
    try:
        # Create and execute the Docker operator
        docker_operator = DockerOperator(
            task_id=task_id,
            **docker_operator_kwargs
        )
        
        # Execute the operator - it will raise an exception if the container exits non-zero
        result = docker_operator.execute(context)
        
        # If we get here, execution was successful
        Variable.set(failure_var, 0)
        return result
        
    except Exception as e:
        if isinstance(e, AirflowSkipException):
            raise  # Re-raise skip exception
        
        # For other exceptions, count as a failure
        failure_count += 1
        Variable.set(failure_var, failure_count)
        
        # Using ti (TaskInstance) from context 
        ti = context.get('ti')
        if ti:
            ti.log.info(f"Docker execution failed: {str(e)}")
        
        if failure_count >= failure_threshold:
            raise Exception(f"Upload failed {failure_count} times in a row: {str(e)}")
        else:
            raise AirflowSkipException(f"Upload failed {failure_count} times: {str(e)}") 