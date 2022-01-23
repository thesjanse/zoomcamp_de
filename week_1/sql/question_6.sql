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
        ytt.total_amount
    FROM
        yellow_taxi_trips ytt
    LEFT JOIN
        territory d
    ON
        ytt.dropoff_location_id = d.id
    LEFT JOIN
        territory p
    ON ytt.pickup_location_id = p.id;

SELECT
    pickup_zone || '/' || dropoff_zone AS trip,
    avg(total_amount) AS avg_price
FROM
    temp_source
GROUP BY
    trip
ORDER BY
    avg_price DESC;