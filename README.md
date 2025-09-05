# Book Recommender 

This app is a semantic book recommendation system with automated ETL pipeline and vector search capabilities.
The whole thing is built around a data pipeline that automatically scrapes book information, processes it, and makes it searchable through semantic similarity. It's not just a simple database lookup - it uses embeddings to understand relationships between concepts and find books that are genuinely similar to what you're describing.


**[Try the App](https://huggingface.co/spaces/davidepanza/Book_Recommender_App)** (hosted on HF Spaces)

Currently few books are available in the database as I'm still testing the app but this will change in the next few weeks.
You can see summaries of the available books at:
**[Book Analytics Dashboard](https://book-recommendation-dashboard.onrender.com)**

## The Data Journey:

- Collection: Airflow schedules jobs that pull book data from Google Books API, handling rate limits and deduplication
- Storage & Processing: Raw data goes to S3, gets cleaned and transformed with dbt, then stored in Athena for querying
- Vectors Generation: SentenceTransformers converts book descriptions into vector embeddings that capture semantic meaning and store them on LanceDB
- Search: When you search, your query gets converted to the same vector space and finds the closest matches
- Interface: React frontend talks to FastAPI backend, which queries the vector database and returns actual recommendations

## Architecture

**Data Flow:**
- **Ingestion:** Airflow → Google Books API → S3 → dbt → Athena
- **Vector Processing:** SentenceTransformers → LanceDB for similarity search  
- **User Interface:** React → FastAPI → LanceDB → Semantic results

## Features

- **Automated Data Pipeline:** Airflow orchestrates book scraping, deduplication, and processing
- **Semantic Search:** Vector embeddings enable intelligent book recommendations
- **Web Interface:** React frontend for intuitive book discovery
- **Scalable Storage:** AWS S3 + Athena for data warehousing
- **Real-time API:** FastAPI backend with vector similarity search

## Tech Stack

- **Orchestration:** Apache Airflow
- **Data Processing:** dbt, AWS S3, AWS Athena
- **ML/Embeddings:** SentenceTransformers (all-MiniLM-L6-v2)
- **Vector Database:** LanceDB
- **Backend:** FastAPI
- **Frontend:** React
- **Data Source:** Google Books API