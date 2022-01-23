SELECT
    pickup_time::date AS pickup_date,
    MAX(tip_amount) max_tip
FROM
    yellow_taxi_trips
WHERE
    date_part('month', pickup_time) = 1 AND
    date_part('year', pickup_time) = 2021
GROUP BY
    pickup_date
ORDER BY
    max_tip DESC
LIMIT 1;