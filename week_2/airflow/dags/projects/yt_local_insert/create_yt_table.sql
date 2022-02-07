CREATE TABLE IF NOT EXISTS {{TABLE}} (
    id SERIAL,
    vendor_id INT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    passenger_count INT,
    trip_distance REAL,
    rate_code_id INT,
    store_fw_flag TEXT,
    pickup_location INT,
    drop_off_location INT,
    payment_type INT,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL
);

TRUNCATE TABLE {{TABLE}};