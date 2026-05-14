-- DDL bootstrap script: create volumes

CREATE VOLUME IF NOT EXISTS nyc_taxi_dev.landing.nyc_taxi_raw;
CREATE VOLUME IF NOT EXISTS nyc_taxi_dev.landing._checkpoints;
CREATE VOLUME IF NOT EXISTS nyc_taxi_dev.landing._schemas;
