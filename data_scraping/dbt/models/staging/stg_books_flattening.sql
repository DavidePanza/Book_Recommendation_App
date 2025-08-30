{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',  
    unique_key='id',
    partitioned_by=['publication_year'],
    tags=['staging'],
    external_location='s3://googlebooks-scraping-data/dbt-output/staging/',
    format='PARQUET'
) }}

SELECT DISTINCT
    id,
    volumeInfo.title as title,
    COALESCE(volumeInfo.subtitle, '') as subtitle,
    CASE 
        WHEN cardinality(volumeInfo.authors) > 0
        THEN volumeInfo.authors[1]
        ELSE 'Unknown Author'
    END as primary_author,
    volumeInfo.authors as all_authors,
    COALESCE(volumeInfo.publisher, 'Unknown Publisher') as publisher,
    -- Robust date parsing - keeps full date
    CASE 
        WHEN TRY(CAST(volumeInfo.publishedDate as DATE)) IS NOT NULL 
        THEN TRY(CAST(volumeInfo.publishedDate as DATE))
        WHEN REGEXP_LIKE(volumeInfo.publishedDate, '^\d{4}-\d{2}$') 
        THEN TRY(CAST(volumeInfo.publishedDate || '-01' as DATE))
        WHEN REGEXP_LIKE(volumeInfo.publishedDate, '^\d{4}$') 
        THEN TRY(CAST(volumeInfo.publishedDate || '-01-01' as DATE))
        ELSE NULL
    END as published_date,
    COALESCE(volumeInfo.description, '') as description,
    COALESCE(volumeInfo.pageCount, 0) as page_count,
    CASE 
        WHEN cardinality(volumeInfo.categories) > 0
            AND LENGTH(volumeInfo.categories[1]) <= 30
        THEN LOWER(volumeInfo.categories[1])
        ELSE NULL
    END as primary_category,
    volumeInfo.categories as all_categories,
    CASE
        WHEN volumeInfo.ratingsCount IS NULL 
            OR volumeInfo.ratingsCount = 0 
            OR volumeInfo.averageRating IS NULL
        THEN NULL
        ELSE volumeInfo.averageRating
    END as avg_rating,
    COALESCE(volumeInfo.ratingsCount, 0) as ratings_count,
    COALESCE(volumeInfo.imageLinks.thumbnail, '') as thumbnail_url,
    COALESCE(volumeInfo.language, 'en') as language,
    COALESCE(volumeInfo.previewLink, '') as preview_link,
    COALESCE(saleInfo.country, 'US') as sale_country,
    COALESCE(saleInfo.saleability, 'NOT_FOR_SALE') as saleability,
    COALESCE(saleInfo.listPrice.amount, 0.0) as list_price,
    COALESCE(saleInfo.listPrice.currencyCode, 'USD') as currency,
    COALESCE(saleInfo.buyLink, '') as buy_link,
    date as processing_date,
    -- MUST BE LAST: Partition column - robust year extraction
    CASE 
        WHEN TRY(CAST(volumeInfo.publishedDate as DATE)) IS NOT NULL 
        THEN YEAR(TRY(CAST(volumeInfo.publishedDate as DATE)))
        WHEN REGEXP_LIKE(volumeInfo.publishedDate, '^\d{4}-\d{2}$') 
        THEN CAST(SUBSTR(volumeInfo.publishedDate, 1, 4) as INTEGER)
        WHEN REGEXP_LIKE(volumeInfo.publishedDate, '^\d{4}$')
        THEN CAST(volumeInfo.publishedDate as INTEGER)
        ELSE NULL
    END as publication_year
FROM {{ source('books_db', 'books_raw_parquet') }}
WHERE date >= '{{ var("start_date", "2025-08-25") }}'
  AND date <= '{{ var("end_date", "2025-08-27") }}'
  AND id IS NOT NULL
  AND volumeInfo.title IS NOT NULL
  AND LENGTH(COALESCE(volumeInfo.description, '')) > 100

{% if is_incremental() %}
  -- Only process data newer than what we already have
  AND date > (SELECT MAX(processing_date) FROM {{ this }})
{% endif %}

{% if var("limit_enabled", false) %}
LIMIT {{ var("authors_limit", 1000) }}
{% endif %}