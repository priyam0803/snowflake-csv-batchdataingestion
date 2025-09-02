# This repository will help you ingest raw data into snowflake from csv files hosted in azure blob.

**1.This framework will perform the following activites for ingesting data into snowflake from csv files in azure blob.**

1.Schema inference of the csv files in azure blob using INFER_SCHEMA()

2.DDL creation based on the schema inference. If the columns names have spaces ,they are replaces with an underscore.The DDLs are stored in a config table

3.Table creation based on the DDL in #2

4.Data ingestion into snowflake tables from the files 

**2.Following are the list of stored procs in the framework:**

GENERATE_CLEAN_DDL : This will infer the schema of the files in azure blob. The inferred schema will be stored in a transient table. Using this inferred schema table , ddl is created and stored in a ddl_details table.
Impacted tables:ddl_details

ITERATE_SOURCEFILELIST_CONFIG : source_filelist_config is a configurable table which contains the details about the file we want to ingest and the table details. This stored proc iterates through each entry in this table and call the stored proc GENERATE_CLEAN_DDL.
Impacted tables:source_filelist_config

CREATE_TABLES : This will create the tables from the ddl in ddl_details table.
Impacted tables:ddl_details

RAW_DATA_INGESTION : This will ingest the data into snowflake based on the  source_filelist_config. An entry gets created in audit_table_inbound_feed with ingestion stats for each file ingested .
Impacted tables:audit_table_inbound_feed,source_filelist_config







