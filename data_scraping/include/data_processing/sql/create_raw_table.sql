CREATE EXTERNAL TABLE IF NOT EXISTS books_db.books_raw_parquet (
    id string,
    volumeInfo struct<
        title: string,
        subtitle: string,
        authors: array<string>,
        publisher: string,
        publishedDate: string,
        description: string,
        pageCount: int,
        categories: array<string>,
        averageRating: double,
        ratingsCount: int,
        imageLinks: struct<thumbnail: string>,
        language: string,
        previewLink: string
    >,
    saleInfo struct<
        country: string,
        saleability: string,
        listPrice: struct<amount: double, currencyCode: string>,
        buyLink: string
    >
) 
PARTITIONED BY (
    date string,
    hour string  
)
STORED AS PARQUET
LOCATION 's3://googlebooks-scraping-data/books/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.date.type' = 'date',
    'projection.date.range' = '2024-01-01,NOW',
    'projection.date.format' = 'yyyy-MM-dd',
    'projection.hour.type' = 'integer',
    'projection.hour.range' = '0,23',
    'projection.hour.digits' = '2'
);