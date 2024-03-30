CREATE EXTERNAL TABLE IF NOT EXISTS Accelerometer (
    user VARCHAR(255),
    timestamp BIGINT,
    x FLOAT,
    y FLOAT,
    z FLOAT
)
LOCATION 's3://project3parent/accelerometer/landing/';
