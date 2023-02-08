-- Create an external table using the fhv 2019 data

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://datalake-w03/fhv/fhv_tripdata_2019-*.csv.gz']
);


-- What is the count for fhv vehicle records for year 2019?

SELECT COUNT(*)
FROM `de-zoomcamp-bizancio3.nytaxi.fhv_tripdata`
WHERE YEAR(pickup_datetime) = 2019

