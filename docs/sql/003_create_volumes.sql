-- DDL bootstrap script: create volumes
-- Replace <CATALOG_NAME> before execution.

CREATE VOLUME IF NOT EXISTS <CATALOG_NAME>.landing.nyc_taxi_raw;
CREATE VOLUME IF NOT EXISTS <CATALOG_NAME>.landing._checkpoints;
CREATE VOLUME IF NOT EXISTS <CATALOG_NAME>.landing._schemas;
