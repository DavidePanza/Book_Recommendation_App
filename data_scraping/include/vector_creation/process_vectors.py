import boto3
import pandas as pd
import os


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
    
    model = get_model()  # Same cached model
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
    """Main processing function with automatic batch detection"""
    from airflow.models import Variable
    import lancedb
    
    # Get config
    config = {
        'aws_key': Variable.get("AWS_ACCESS_KEY_ID"),
        'aws_secret': Variable.get("AWS_SECRET_ACCESS_KEY"), 
        'region': Variable.get("AWS_REGION", "us-east-1"),
        'bucket': Variable.get("S3_BUCKET"),
        'key': Variable.get("S3_KEY"),
        'table_name': Variable.get("LANCEDB_TABLE", "vectors"),
        'lancedb_uri': Variable.get("LANCEDB_URI"),
        'lancedb_api_key': Variable.get("LANCEDB_API_KEY", None)
    }
    
    # Setup connections
    s3 = boto3.client('s3', aws_access_key_id=config['aws_key'],
                     aws_secret_access_key=config['aws_secret'], region_name=config['region'])
    
    if config['lancedb_api_key']:
        os.environ["LANCEDB_API_KEY"] = config['lancedb_api_key']
    db = lancedb.connect(config['lancedb_uri'])
    
    # Load data
    obj = s3.get_object(Bucket=config['bucket'], Key=config['key'])
    df = pd.read_parquet(obj['Body'])
    
    # Process data
    df['processed_text'] = df.get('description', '').fillna('').astype(str)
    df = df[df['processed_text'].str.strip() != '']
    
    print(f"Loaded {len(df)} records")
    
    # Check table mode
    try:
        db.open_table(config['table_name'])
        mode = "append"
    except:
        mode = "overwrite"
    
    # Batch decision
    if len(df) > 500:
        print("Using batch processing")
        return process_in_batches(df, db, config['table_name'], mode)
    else:
        print("Using normal processing") 
        return process_normal(df, db, config['table_name'], mode)