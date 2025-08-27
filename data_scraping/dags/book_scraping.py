from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import sqlite3
import os
import json
import glob
import boto3
import pandas as pd  
from io import BytesIO  
from datetime import datetime
from include.book_scraping.deduplication import deduplicate_books   
from include.book_scraping.scraping import get_filtered_books


@dag(
    'book_scraping_pipeline',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    params={
        "reset_from_start": False,  # Parameter to control restart behavior
        "authors_to_process": 5
    }
)
def book_scraping_pipeline():

    @task
    def cleanup_temp_folder():
        """Clean up all temp files at the start of processing"""
        temp_dir = "/opt/airflow/data/temp"
        
        try:
            # Create temp directory if it doesn't exist
            os.makedirs(temp_dir, exist_ok=True)
            
            # Remove all files in temp directory
            temp_files = glob.glob(f"{temp_dir}/*")
            
            if temp_files:
                for temp_file in temp_files:
                    if os.path.isfile(temp_file):
                        os.remove(temp_file)
                        print(f"Cleaned up old temp file: {temp_file}")
                print(f"Cleaned up {len(temp_files)} temp files")
            else:
                print("No temp files to clean up")
                
        except Exception as e:
            print(f"Warning: Could not clean temp directory: {e}")
        
        return "Temp cleanup completed"

    @task
    def init_database():
        """Initialize SQLite database if it doesn't exist"""
        db_path = "/opt/airflow/data/output/books_metadata.db"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        
        # Create tables
        conn.execute('''
            CREATE TABLE IF NOT EXISTS processed_authors (
                processing_order INTEGER PRIMARY KEY,
                author_name TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_books_count INTEGER DEFAULT 0,
                deduplicated_books_count INTEGER DEFAULT 0,
                books_found INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        
        return db_path

    @task
    def get_last_processed_author():
        """Find where to resume processing"""
        db_path = "/opt/airflow/data/output/books_metadata.db"
        
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT MAX(processing_order) FROM processed_authors"
        )
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result is not None else None
    
    @task()
    def fetch_books_task():
        """Fetch books for authors in the current processing batch."""

        # Get processing variables
        context = get_current_context()
        authors_count = context['params'].get('authors_to_process', 10)
        last_author_processed_idx = context['ti'].xcom_pull(task_ids='get_last_processed_author')

        # Load authors from file
        authors_path = "/opt/airflow/data/input/authors_filtered.txt"
        with open(authors_path, 'r') as f:
            authors = [line.strip() for line in f.readlines()]

        # Set start and stop indices for author processing
        if last_author_processed_idx:
            start_index = last_author_processed_idx + 1
        else: 
            start_index = 0
        stop_index = start_index + authors_count
        cur_index = start_index

        # Output variables
        metadata = {
            "processing_order": [],
            "author_name": [],
            "processed_date": [],
            "raw_books_count": []  # Added this
        }
        raw_books_list = []

        for author in authors[start_index:stop_index]:

            print(f"Processing author: {author}")
            raw_books = get_filtered_books(author, 10)
            raw_books_list.append(raw_books)

            print(f"Found {len(raw_books)} unique books for author: {author}\n")
            metadata["processing_order"].append(cur_index)
            metadata["author_name"].append(author)
            metadata["processed_date"].append(datetime.now())
            metadata["raw_books_count"].append(len(raw_books))
            cur_index += 1
        
        # Create temp file path with timestamp for uniqueness
        run_id = context['run_id']
        temp_dir = "/opt/airflow/data/temp"
        temp_file = f"{temp_dir}/raw_books_{run_id}.json"

        # Store raw data in temporary file
        with open(temp_file, 'w') as f:
            json.dump({
                'raw_books_list': raw_books_list,  
                'metadata': metadata
            }, f, default=str) 
        
        print(f"Stored raw books for {len(raw_books_list)} authors in {temp_file}")

        # Return just the file path (small data for XCom)
        return temp_file

    @task()
    def deduplicate_books_task():
        """Deduplicate books for authors in the current processing batch."""

        # Get raw data and processing metadata
        context = get_current_context()
        temp_file_path = context['ti'].xcom_pull(task_ids='fetch_books_task')
        with open(temp_file_path, 'r') as f:
            books_list = json.load(f).get('raw_books_list', [])
        with open(temp_file_path, 'r') as f:
            metadata = json.load(f).get('metadata', {})

        # Initialize counters
        deduplicated_books_count = []
        books_found = []
        all_ids = []

        # Deduplicate books
        deduplicated_books = []
        for idx in range(len(books_list)):
            books_output = deduplicate_books(books_list[idx], verbose=True)
            ids = [books_output[idx]['id'] for idx in range(len(books_output))]
            deduplicated_books.extend(books_output)
            all_ids.extend(ids)

            # Count deduplicated books
            deduplicated_books_count.append(len(books_output))
            book_found = int(len(books_output) > 0)
            books_found.append(book_found)

        if len(deduplicated_books_count) != len(metadata["raw_books_count"]):
            raise ValueError("Mismatch in deduplicated books count and metadata length")

        metadata["books_found"] = books_found
        metadata["deduplicated_books_count"] = deduplicated_books_count

        # Store deduplicated books in temp file
        run_id = context['run_id']
        temp_dir = "/opt/airflow/data/temp"
        temp_file_books = f"{temp_dir}/deduplicated_books_{run_id}.json"
        temp_file_ids = f"{temp_dir}/deduplicated_ids_{run_id}.json"

        with open(temp_file_books, 'w') as f:
            json.dump(deduplicated_books, f, default=str)

        with open(temp_file_ids, 'w') as f:
            json.dump(all_ids, f, default=str)

        # Store METADATA in SQLite database
        db_path = "/opt/airflow/data/output/books_metadata.db"
        
        try:
            rows = []
            for po, name, pdate, raw_cnt, dedup_cnt, found in zip(
                metadata['processing_order'],
                metadata['author_name'],
                metadata['processed_date'],
                metadata['raw_books_count'],
                deduplicated_books_count,
                books_found
            ):
                rows.append((po, name, pdate, raw_cnt, dedup_cnt, found))

            sql = """
            INSERT OR REPLACE INTO processed_authors
            (processing_order, author_name, processed_date, raw_books_count, deduplicated_books_count, books_found)
            VALUES (?, ?, ?, ?, ?, ?)
            """

            with sqlite3.connect(db_path) as conn:
                conn.executemany(sql, rows) 
            print(f"Stored metadata for {len(metadata['author_name'])} authors in SQLite database")
        
        except Exception as e:
            print(f"Error storing metadata in database: {e}")
            raise e

        return {
            'books_file': temp_file_books,
            'ids_file': temp_file_ids
            }
    
    @task()
    def upload_to_s3_parquet():
        """Upload data directly as Parquet to S3"""
        bucket_name = "googlebooks-scraping-data"
        s3_client = boto3.client('s3')
        
        # Create bucket if needed
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except:
            s3_client.create_bucket(Bucket=bucket_name)
        
        # Get file paths from XCom
        context = get_current_context()
        file_paths = context['ti'].xcom_pull(task_ids='deduplicate_books_task')
        
        books_file = file_paths['books_file']
        ids_file = file_paths['ids_file']
        
        # Create timestamp for partitioning
        timestamp = datetime.now()
        date_partition = timestamp.strftime('%Y-%m-%d')
        hour_partition = timestamp.strftime('%H')
        file_timestamp = timestamp.strftime('%Y%m%d_%H%M%S')
        
        # Convert and upload books data
        books_df = pd.read_json(books_file)
        books_buffer = BytesIO()
        books_df.to_parquet(books_buffer, index=False)
        books_buffer.seek(0)
        
        s3_client.upload_fileobj(
            books_buffer, 
            bucket_name, 
            f"books/date={date_partition}/hour={hour_partition}/books_{file_timestamp}.parquet"
        )
        
        # Convert and upload IDs data
        ids_df = pd.read_json(ids_file)
        ids_buffer = BytesIO()
        ids_df.to_parquet(ids_buffer, index=False)
        ids_buffer.seek(0)
        
        s3_client.upload_fileobj(
            ids_buffer, 
            bucket_name, 
            f"ids/date={date_partition}/hour={hour_partition}/ids_{file_timestamp}.parquet"
        )
        
        return f"Upload complete: {file_timestamp}"


    cleanup_temp_folder() >> init_database() >> get_last_processed_author()  >> fetch_books_task() >> deduplicate_books_task() >> upload_to_s3_parquet()

book_scraping_pipeline()
