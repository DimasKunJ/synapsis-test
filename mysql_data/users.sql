-- Create a new user to be used on ETL

CREATE USER 'etl_user'@'%' IDENTIFIED BY 'etlpassword123';

GRANT USAGE ON `coal_mining`.* TO 'etl_user'@'%';

GRANT SELECT ON `coal_mining`.`mines` TO 'etl_user'@'%';
GRANT SELECT ON `coal_mining`.`production_logs` TO 'etl_user'@'%';

FLUSH PRIVILEGES;