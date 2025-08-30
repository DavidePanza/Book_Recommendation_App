from pathlib import Path

def load_sql(filename, **kwargs):
    """Load SQL file and substitute variables"""
    sql_path = Path(__file__).parent / 'sql' / filename
    sql = sql_path.read_text()
    return sql.format(**kwargs) if kwargs else sql

def initialize_s3_folders():
    """Initialise S3 folders for storing dbt results"""
    import boto3
    import os
    
    bucket_name = os.getenv('S3_BUCKET', 'googlebooks-scraping-data')
    s3_client = boto3.client('s3')
    
    folders_to_create = [
        "dbt-output/staging/",
        "dbt-output/lookup/",
        # "athena-results/"  # already created
    ]
    
    for folder in folders_to_create:
        try:
            # Create folder by putting empty object 
            s3_client.put_object(
                Bucket=bucket_name, 
                Key=folder,
                Body=b'',
                ContentType='application/x-directory'
            )
            print(f"Ensured folder exists: {folder}")
        except Exception as e:
            print(f"Error creating {folder}: {e}")
    
    return "S3 folder structure initialized"