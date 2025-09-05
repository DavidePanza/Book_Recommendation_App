from airflow.decorators import dag, task
from datetime import datetime
from include.vector_creation.process_vectors import process_vectors
from include.vector_creation.validate_inputs import validate_inputs


@dag(
    'vector_ingestion',
    start_date=datetime(2024, 1, 1),
    description='Process S3 parquet files and store embeddings in LanceDB Cloud',
    schedule_interval=None,  
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": "2025-08-28",
        "end_date": "2025-08-29"
    }
)
def create_dag():

    @task
    def validate_task():
        """Validate required Airflow Variables"""
        result = validate_inputs()
        print(f"Validation result: {result}")
        return result

    @task
    def process_task():
        """Process S3 data and store in LanceDB with batch handling"""
        result = process_vectors()
        print(f"Processing result: {result}")
        return result

    # Set dependencies
    validate_result = validate_task()
    process_result = process_task()
    
    # Chain the tasks
    validate_result >> process_result

# Create the DAG
dag_instance = create_dag()