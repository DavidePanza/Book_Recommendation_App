from airflow.models import Variable

def validate_inputs(**context):
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET", "S3_KEY", "LANCEDB_URI", "LANCEDB_API_KEY", "LANCEDB_TABLE"]
    for var in required_vars:
        try:
            Variable.get(var)
        except Exception:
            raise ValueError(f"Required Airflow Variable '{var}' not found")