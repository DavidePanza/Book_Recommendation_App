import boto3
import pandas as pd
import os
from datetime import datetime, timedelta

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# Model cache at module level
_model_cache = None

def get_model():
    """Get cached model or load it once"""
    global _model_cache
    if _model_cache is None:
        from sentence_transformers import SentenceTransformer
        print("Loading SentenceTransformer model...")
        _model_cache = SentenceTransformer("all-MiniLM-L6-v2")
        print("Model loaded and cached")
    return _model_cache


def list_s3_parquet_files(s3_client, bucket, start_date, end_date, prefix="dbt-output/staging/"):
    """List parquet files in S3 within date range"""
    files = []
    all_files = []  # Debug: collect all files to see what's there
    
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            all_files.append(key)  # Debug: track all files
            
            # Skip directories and check if it looks like a parquet file
            if key.endswith('/') or key.split('/')[-1] == '':
                continue
                
            # Extract date from path like dbt-output/staging/processing_date=2025-08-29/...
            if '/processing_date=' in key:
                date_part = key.split('/processing_date=')[1].split('/')[0]
                print(f"Found file with processing_date={date_part}: {key}")
                if start_date <= date_part <= end_date:
                    files.append(key)
    
    # Debug output
    print(f"Total files in {prefix}: {len(all_files)}")
    if len(all_files) <= 10:  # Show all if small number
        for f in all_files:
            print(f"  - {f}")
    else:  # Show first few
        print("Sample files:")
        for f in all_files[:5]:
            print(f"  - {f}")
        print(f"  ... and {len(all_files)-5} more")
    
    return files


def load_multiple_parquet_files(s3_client, bucket, file_keys):
    """Load and combine multiple parquet files from S3"""
    from io import BytesIO
    dfs = []
    
    for key in file_keys:
        print(f"Loading: {key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        # Read the stream into BytesIO to make it seekable
        parquet_buffer = BytesIO(obj['Body'].read())
        df = pd.read_parquet(parquet_buffer)
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No files found in date range")
        
    return pd.concat(dfs, ignore_index=True)


def process_normal(df, db, table_name, mode):
    """Normal processing for datasets <= 500 records"""
    import pyarrow as pa
    
    model = get_model()
    texts = [str(t) for t in df['processed_text']]
    embeddings = model.encode(texts)
    df['vector'] = embeddings.tolist()
    
    if mode == "overwrite":
        db.create_table(table_name, pa.Table.from_pandas(df), mode="overwrite")
    else:
        table = db.open_table(table_name)
        table.add(pa.Table.from_pandas(df))
        
    return len(df)


def process_in_batches(df, db, table_name, initial_mode):
    """Batch processing for datasets > 500 records"""
    import pyarrow as pa
    
    model = get_model()
    batch_size = 500
    total_processed = 0
    current_mode = initial_mode
    
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size].copy()
        
        texts = [str(t) for t in batch_df['processed_text']]
        embeddings = model.encode(texts)
        batch_df['vector'] = embeddings.tolist()
        
        if current_mode == "overwrite":
            db.create_table(table_name, pa.Table.from_pandas(batch_df), mode="overwrite")
        else:
            table = db.open_table(table_name)
            table.add(pa.Table.from_pandas(batch_df))
        
        total_processed += len(batch_df)
        print(f"Batch {i//batch_size + 1}, records: {len(batch_df)}")
        current_mode = "append"
    
    return total_processed


def process_vectors():
    """Main processing function with date range support"""
    from airflow.operators.python import get_current_context
    import lancedb
    
    # Get context for params
    context = get_current_context()
    
    # Get config - hardcoded for now (replace with your values)
    config = {
        'aws_key': os.getenv("AWS_ACCESS_KEY_ID", "your-aws-access-key"),
        'aws_secret': os.getenv("AWS_SECRET_ACCESS_KEY", "your-aws-secret"), 
        'region': os.getenv("AWS_REGION", "us-east-1"),
        'bucket': os.getenv("S3_BUCKET", "googlebooks-scraping-data"),
        'start_date': context['params'].get('start_date'),
        'end_date': context['params'].get('end_date'),
        'table_name': os.getenv("LANCEDB_TABLE", "vectors"),
        'lancedb_uri': os.getenv("LANCEDB_URI", "your-lancedb-uri"),
        'lancedb_api_key': os.getenv("LANCEDB_API_KEY", "your-lancedb-api-key")
    }
    
    # Setup connections
    s3 = boto3.client('s3', aws_access_key_id=config['aws_key'],
                     aws_secret_access_key=config['aws_secret'], region_name=config['region'])
    
    if config['lancedb_api_key']:
        os.environ["LANCEDB_API_KEY"] = config['lancedb_api_key']
    db = lancedb.connect(config['lancedb_uri'])
    
    # Get parquet files in date range
    parquet_files = list_s3_parquet_files(
        s3, config['bucket'], config['start_date'], config['end_date']
    )
    
    print(f"Found {len(parquet_files)} parquet files in date range {config['start_date']} to {config['end_date']}")
    print("Files found:")
    for file in parquet_files:
        print(f"  - {file}")
    
    if not parquet_files:
        return 0
    
    # Load and combine data
    df = load_multiple_parquet_files(s3, config['bucket'], parquet_files)
    
    print(f"Loaded {len(df)} records from {len(parquet_files)} files")
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Check specifically for description column
    if 'description' in df.columns:
        print(f"Description column found with type: {df['description'].dtype}")
        
        # Apply your cleaning function
        from include.vector_creation.clean_data import clean_data
        df = clean_data(df)
        
        df['processed_text'] = df['description'].fillna('').astype(str)
        
        # Filter out empty text
        df = df[df['processed_text'].str.strip() != '']
        
    else:
        print(f"ERROR: 'description' column not found!")
        print(f"Available columns: {list(df.columns)}")
        return 0
    
    print(f"Loaded {len(df)} records after filtering")
    
    # Check table mode
    try:
        db.open_table(config['table_name'])
        mode = "append"
    except:
        mode = "overwrite"
    
    # Process with batch decision
    if len(df) > 500:
        print("Using batch processing")
        return process_in_batches(df, db, config['table_name'], mode)
    else:
        print("Using normal processing") 
        return process_normal(df, db, config['table_name'], mode)