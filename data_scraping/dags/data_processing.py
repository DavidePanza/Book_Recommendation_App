from datetime import datetime
import os
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from include.data_processing.utils import load_sql, initialize_s3_folders


@dag(
    'book_etl_pipeline',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "reset_from_start": False,
        "authors_to_process": 5,
        "start_date": "2025-09-08",
        "end_date": "2025-09-10",
    },
    tags=['books', 'etl', 'parquet']
)
def book_etl_pipeline():
    """
    Process existing Parquet book data from S3 using Athena and dbt
    Your data structure: array of arrays containing book objects with nested JSON
    """

    # Load SQL
    create_db_sql = load_sql('create_raw_db.sql')
    create_table_sql = load_sql('create_raw_table.sql')
    repair_partitions_sql = load_sql('repair_partitions.sql')

    @task
    def initialize_s3_folders_task():
        """ Initialize necessary S3 folders """
        result = initialize_s3_folders() 
        return result 

    create_database = AthenaOperator(
        task_id='create_database',
        query=create_db_sql,
        database='default',
        output_location='s3://googlebooks-scraping-data/athena-results/',
        region_name=os.getenv('AWS_REGION', 'us-east-1'),
    )
    
    create_table = AthenaOperator(
        task_id='create_table',
        query=create_table_sql,
        database='books_db',
        output_location='s3://googlebooks-scraping-data/athena-results/',
        region_name=os.getenv('AWS_REGION', 'us-east-1'),
    )

    # you don't need this
    # repair_partitions = AthenaOperator(
    #     task_id='repair_partitions',
    #     query=repair_partitions_sql,
    #     database='books_db',
    #     output_location='s3://googlebooks-scraping-data/athena-results/',
    #     region_name=os.getenv('AWS_REGION', 'us-east-1'),
    # )

    dbt_run_staging = DbtRunOperator(
        task_id="dbt_run_staging",
        dir="/opt/airflow/dbt",  # path to your local dbt project
        profiles_dir="/opt/airflow/dbt",  # path to profiles.yml
        select="tag:staging",  # equivalent of --models tag:staging
        vars={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
            "authors_limit": "{{ params.authors_to_process }}"
        }
    )

    initialize_s3_folders_task() >> create_database >> create_table >> dbt_run_staging

# Instantiate the DAG
book_dag = book_etl_pipeline()


# Find specific book by ID (efficient)
# SELECT bf.* 
# FROM books_id_lookup bil
# JOIN books_flattened bf ON bil.id = bf.id AND bil.publication_year = bf.publication_year
# WHERE bil.id = 'abc123';