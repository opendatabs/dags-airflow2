"""
Custom operators with built-in failure tracking capabilities.
"""

from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from typing import Dict, Any, Optional
from datetime import timedelta


class FailureTrackingDockerOperator(DockerOperator):
    """
    DockerOperator with built-in failure tracking.
    
    This operator extends the standard DockerOperator to include failure tracking,
    which allows a certain number of consecutive failures before actually failing the task.
    """
    
    def __init__(
        self,
        *,
        failure_threshold: int,
        execution_timeout: Optional[timedelta],
        **kwargs,
    ) -> None:
        """
        Initialize the FailureTrackingDockerOperator.
        
        Args:
            failure_threshold: Number of failures at which the task will fail
                               (1 = immediate failure with no skipping,
                                2 = skip on first failure, fail on second,
                                3 = skip on first and second failures, fail on third, etc.)
            execution_timeout: Maximum time allowed for the execution of this task instance,
                               expressed as a timedelta object, e.g. timedelta(minutes=30).
                               Use None for no timeout.
            **kwargs: Arguments to pass to the parent DockerOperator
        """
        super().__init__(execution_timeout=execution_timeout, **kwargs)
        self.failure_threshold = failure_threshold
        
    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the docker container with failure tracking.
        
        Args:
            context: Airflow context
            
        Returns:
            The result of the docker execution
            
        Raises:
            AirflowSkipException: If failure count is under threshold
            Exception: If failure count reaches or exceeds threshold
        """
        # Get failure variable name
        dag_id = self.dag_id
        task_id = self.task_id
        failure_var = f"{dag_id}_consecutive_failures"
        
        # Get failure count
        failure_count = int(Variable.get(failure_var, default_var=0))
        
        try:
            # Execute the parent operator
            result = super().execute(context)
            
            # If we get here, execution was successful
            Variable.set(failure_var, 0)
            return result
            
        except Exception as e:
            if isinstance(e, AirflowSkipException):
                raise  # Re-raise skip exception
            
            # For other exceptions, count as a failure
            failure_count += 1
            Variable.set(failure_var, failure_count)
            
            self.log.info(f"Docker execution failed: {str(e)}")
            
            # Task fails ON the Nth failure (when count reaches threshold)
            if failure_count >= self.failure_threshold:
                raise Exception(f"Upload failed {failure_count} times in a row: {str(e)}")
            else:
                raise AirflowSkipException(f"Upload failed {failure_count} times, skipping task: {str(e)}") 