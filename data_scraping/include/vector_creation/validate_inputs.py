import os

def validate_inputs():
    """Validate required environment variables"""
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY', 
        'S3_BUCKET',
        'LANCEDB_URI'
    ]
    
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Required environment variable '{var}' not found")
    
    return "All required environment variables found"