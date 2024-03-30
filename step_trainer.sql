CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer (
    sensorReadingTime BIGINT,
    serialNumber VARCHAR(50),
    distanceFromObject INT
)
LOCATION 's3://project3parent/step_trainer/landing/';
