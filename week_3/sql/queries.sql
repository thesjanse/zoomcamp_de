CREATE OR REPLACE EXTERNAL TABLE trips_data_all.fhv_2019
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_zoom-338716/raw/fhv_2019_*.parquet']
);

-- Question 1
SELECT COUNT(*) FROM trips_data_all.fhv_2019;

-- Question 2
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM trips_data_all.fhv_2019;

-- Question 4
CREATE OR REPLACE TABLE trips_data_all.fhv_2019_pc
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM trips_data_all.fhv_2019;

SELECT
    COUNT(*)
FROM
    trips_data_all.fhv_2019_pc
WHERE
    dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31' AND
    dispatching_base_num IN ('B00987', 'B02060', 'B02279');