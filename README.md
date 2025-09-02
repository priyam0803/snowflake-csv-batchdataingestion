# This repository will help you ingest raw data into snowflake from csv files hosted in azure blob.

As a pre-requisite , we have to create the following:

**1. Create a database in snowflake**

CREATE DATABASE customers;

**2. Create a Storage integration in snowflake**

CREATE STORAGE INTEGRATION azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '875fbdae-6aae-444c-80ad-676c43e6f1b5'
  STORAGE_ALLOWED_LOCATIONS = ('azure://databricksazure.blob.core.windows.net/databricksazure/');;

  DESCRIBE INTEGRATION azure_int;
  SHOW STORAGE INTEGRATIONS;
  
  --VALIDATE STORAGE INT
  SELECT
  SYSTEM$VALIDATE_STORAGE_INTEGRATION(
    'AZURE_INT',
    'azure://databricksazure.blob.core.windows.net/databricksazure/',
    'test.csv', 'all')
    
**3. Create file formats in snowflake. There are 2 file formats created, one(csv_format_inferschema) which will be used to infer the schema of the csv file in azure blob, and the other file format(csv_format_fieload) for data ingestion into snowflake**
   
CREATE or replace FILE FORMAT csv_format_inferschema 
  TYPE = 'CSV' 
  FIELD_DELIMITER = ',' 
  RECORD_DELIMITER = '\n' 
  PARSE_HEADER = true;
  ;

CREATE or replace FILE FORMAT csv_format_fieload 
  TYPE = 'CSV' 
  FIELD_DELIMITER = ',' 
  RECORD_DELIMITER = '\n' 
  skip_header = 1;
  ;  


**4. Create a stage in snowflake**

CREATE or replace STAGE azure_stage
  STORAGE_INTEGRATION = azure_int
  URL = 'azure://databricksazure.blob.core.windows.net/databricksazure/'
  FILE_FORMAT = csv_format_inferschema;


**5. Create an event table in snowflake. This will capture all the telemetry data in snowflake**

USE SCHEMA customers.public;

CREATE EVENT TABLE customers.public.azure_fileingestion_events;

ALTER DATABASE customers SET EVENT_TABLE = customers.public.azure_fileingestion_events;

ALTER DATABASE customers SET LOG_LEVEL = INFO;

SHOW PARAMETERS LIKE 'event_table' IN DATABASE customers;   
 

 
