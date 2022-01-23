SELECT
    COUNT(1) AS trips
FROM
    yellow_taxi_trips
WHERE
    pickup_time::date = '2021-01-15';