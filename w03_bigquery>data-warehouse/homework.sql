-- Create an external table using the fhv 2019 data

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-bizancio3.nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://datalake-w03/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Create a BQ table using the fhv 2019 data (no partition no cluster)

CREATE OR REPLACE TABLE `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_non_partitoned` AS
SELECT * FROM `de-zoomcamp-bizancio3.nytaxi.external_fhv_tripdata`;


-- What is the count for fhv vehicle records for year 2019?

SELECT COUNT(*) as count
FROM `de-zoomcamp-bizancio3.external_nytaxi.fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019

Answer: 43244696


-- Count the distinct number of affiliated_base_number for the entire dataset
-- (both for external and bigquery table)

SELECT COUNT(DISTINCT affiliated_base_number) as count
FROM `de-zoomcamp-bizancio3.nytaxi.external_fhv_tripdata`

SELECT COUNT(DISTINCT affiliated_base_number) as count
FROM `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_non_partitoned`

(# of distinct affiliated_base_number: 3165)

-- What is the estimated amount of data that will be read 
-- when this query is executed on the external table and the bigquery table?

Answer: 0 MB for the External Table and 317.94MB for the BQ Table


-- How many records have both a blank (null) PUlocationID and DOlocationID?

SELECT COUNT(*) as count
FROM `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_non_partitoned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL

Answer: 717748


-- What is the best strategy to optimize the table 
-- if query always filter by pickup_datetime and order by affiliated_base_number?

  Partitioning the table based on the pickup_datetime column 
  will allow BigQuery to efficiently filter the data based on the partition, 
  reducing the amount of data that needs to be scanned for each query. 

  Clustering the table based on the affiliated_base_number column 
  will physically store the data in the order of the clustering column, 
  allowing BigQuery to perform an efficient range scan when ordering the data based on this column. 

Answer: Partition by pickup_datetime Cluster on affiliated_base_number


-- Implement the optimized solution you chose for question 4

CREATE OR REPLACE TABLE `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `de-zoomcamp-bizancio3.nytaxi.external_fhv_tripdata`;

-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive)
-- Compare query performace non_partitoned vs. partitoned table

SELECT DISTINCT affiliated_base_number
FROM `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_non_partitoned`
WHERE pickup_datetime BETWEEN '2019-03-01 00:00:00' AND '2019-03-31 23:59:59'

SELECT DISTINCT affiliated_base_number
FROM `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata_partitoned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01 00:00:00' AND '2019-03-31 23:59:59'

Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table


-- Where is the data stored in the External Table you created?
Answer: GCP Bucket


-- True or False: It is best practice in Big Query to always cluster your data?

  In general, it's a good idea to consider the usage patterns of your data and queries 
  before deciding whether to cluster your data in BigQuery. 
  If you have a large, complex dataset with a high volume of queries that require 
  efficient range scans, clustering may be a good option. 
  If your data and queries are simpler, clustering may not be necessary

Answer: False

