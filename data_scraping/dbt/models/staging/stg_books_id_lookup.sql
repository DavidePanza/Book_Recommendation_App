{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',  
    unique_key='id',
    tags=['staging'],
    external_location='s3://googlebooks-scraping-data/dbt-output/lookup/',
    format='PARQUET'
) }}

SELECT 
    id,
    title,
    primary_author,
    publication_year,
    '{{ var("processing_date") }}' as processing_date
FROM {{ ref('stg_books_flattening') }} 
WHERE id IS NOT NULL

