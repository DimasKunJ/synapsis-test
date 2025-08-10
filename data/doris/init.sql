CREATE DATABASE IF NOT EXISTS coal_mining_dwh;
USE coal_mining_dwh;

CREATE TABLE IF NOT EXISTS daily_production_metrics (
   `date` DATE NOT NULL,
   `total_production_daily` DECIMAL(18, 2),
   `average_quality_grade` DECIMAL(5, 2),
   `equipment_utilization` DOUBLE,
   `total_fuel_consumption` DECIMAL(18, 2),
   `mean_temperature` DOUBLE,
   `total_precipitation` DOUBLE,
   `fuel_efficiency` DOUBLE
)
DUPLICATE KEY(`date`)
DISTRIBUTED BY HASH(`date`)
PROPERTIES("replication_num" = "1");

CREATE TABLE IF NOT EXISTS anomaly_production_log (
   `date` DATE NOT NULL,
   `mine_id` INT,
   `shift` VARCHAR(10),
   `tons_extracted` DECIMAL(10, 2),
   `quality_grade` DECIMAL(3, 1)
)
DISTRIBUTED BY HASH(`mine_id`)
PROPERTIES("replication_num" = "1");

CREATE TABLE IF NOT EXISTS anomaly_iot_log (
    `date` DATE NOT NULL,
    `equipment_utilization` DOUBLE,
    `total_fuel_consumption` DECIMAL(10, 2)
)
DISTRIBUTED BY HASH(`date`)
PROPERTIES("replication_num" = "1");


CREATE TABLE IF NOT EXISTS anomaly_weather_log (
    `date` DATE NOT NULL,
    `mean_temperature` DOUBLE,
    `total_precipitation` DOUBLE
)
DISTRIBUTED BY HASH(`date`)
PROPERTIES("replication_num" = "1");

-- Create password
CREATE USER 'superset_user'@'%' IDENTIFIED BY 'superset123';

GRANT SELECT_PRIV ON coal_mining_dw.* TO 'superset_user'@'%';