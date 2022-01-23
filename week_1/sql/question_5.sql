CREATE TEMP TABLE temp_source AS
    SELECT
        CASE
            WHEN p."zone" IN ('NA', 'NV', NULL)
            THEN 'Unknown'
            ELSE p."zone" END AS pickup_zone,
        CASE
            WHEN d."zone" IN ('NA', 'NV', NULL)
            THEN 'Unknown'
            ELSE d."zone" END AS dropoff_zone,
        ytt.pickup_time
    FROM
        yellow_taxi_trips ytt
    LEFT JOIN
        territory d
    ON
        ytt.dropoff_location_id = d.id
    LEFT JOIN
        territory p
    ON
        ytt.pickup_location_id = p.id;

SELECT
    dropoff_zone,
    COUNT(1) AS trips
FROM
    temp_source
WHERE
    pickup_time::date = '2021-01-14' AND
    pickup_zone = 'Central Park'
GROUP BY
    dropoff_zone
ORDER BY
    trips DESC
LIMIT 1;