CREATE TABLE kocakale_homework.gold_allviews
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://ceu-yahya-wikidata/datalake/gold_allviews/'
) AS
SELECT
  article,
  SUM(views) AS total_top_view,
  MIN(rank) AS top_rank,
  COUNT(DISTINCT date) AS ranked_days
FROM
  kocakale_homework.silver_views
GROUP BY
  article
ORDER BY
  total_top_view DESC;
