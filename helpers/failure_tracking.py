"""
Utility module for handling failure tracking in DAGs.
"""

from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from typing import Dict, Any, Optional


def execute_docker_with_failure_tracking(
    dag_id: str,
    task_id: str,
    failure_threshold: int,
    docker_operator_kwargs: Dict[str, Any],
    context: Dict[str, Any],
    failure_var_name: Optional[str] = None,
) -> Any:
    """
    Wrapper function that executes Docker container and handles failures with a threshold
    
    Parameters:
        dag_id: The ID of the DAG
        task_id: Task identifier
        failure_threshold: Number of consecutive failures allowed before actually failing
        docker_operator_kwargs: Arguments to pass to the DockerOperator
        context: The Airflow task context
        failure_var_name: Optional custom name for the failure variable
    
    Returns:
        Result from the Docker execution
    
    Raises:
        AirflowSkipException: If failure count is under threshold
        Exception: If failure count exceeds threshold
    """
    # Get or create failure variable name
    failure_var = failure_var_name if failure_var_name else f"{dag_id}_consecutive_failures"
    
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
        
        context['ti'].log.info(f"Docker execution failed: {str(e)}")
        
        if failure_count >= failure_threshold:
            raise Exception(f"Upload failed {failure_count} times in a row: {str(e)}")
        else:
            raise AirflowSkipException(f"Upload failed {failure_count} times: {str(e)}") 