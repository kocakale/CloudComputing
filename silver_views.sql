CREATE TABLE kocakale_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://ceu-yahya-wikidata/datalake/views_silver/'
    ) AS SELECT article, views, rank, date FROM kocakale_homework.bronze_views