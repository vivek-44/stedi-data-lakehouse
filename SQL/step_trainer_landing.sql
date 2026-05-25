CREATE EXTERNAL TABLE step_trainer_landing(
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-vivek/step_trainer/landing/'
