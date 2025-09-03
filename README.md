# This repository will help you ingest raw data into snowflake from csv files hosted in azure blob.

**1.This framework will perform the following activites for ingesting data into snowflake from csv files in azure blob.**

a.  Schema inference of the csv files in azure blob using INFER_SCHEMA()

b.  DDL creation based on the schema inference. If the columns names have spaces ,they are replaces with an underscore.The DDLs are stored in a config table

c.  Table creation based on the DDL in #b

d.  Data ingestion into snowflake tables from the files 

**2.Following are the list of tables used in the framework:**

a.  source_filelist_config : This table contains the details about the file we want to ingest and the table details e.g stage name, file name, file formats, target table details, config table details. This table contains 2 flags:

DDL_GENERATE_FLAG - To denote if the DDL needs to be created by inferring the schema of the flag. e.g (Y/N)

ACTIVE_FLAG - To denote if the entry is active or not. e.g (Y/N)

b.  ddl_details : This table contains the ddl generated for the table. This table contains 2 flags: 

ACTIVE_FLAG - To denote if the entry is active or not. e.g (Y/N). When re-creating the DDL for the same table, the previous entry is invalidated as 'N'. 

TABLE_CREATED_FLAG - To denote if the target table is created using the DDL. e.g (Y/N). When Y, then the table is not created using the DDL

c.  audit_table_inbound_feed : This is an audit table which captures the ingestion stats for the files ingested.

**3.Following are the list of stored procs in the framework:**

a.  GENERATE_CLEAN_DDL : This will infer the schema of the files in azure blob. The inferred schema will be stored in a transient table. Using this inferred schema table , ddl is created and stored in a ddl_details table.

Impacted tables:ddl_details

b.  ITERATE_SOURCEFILELIST_CONFIG : source_filelist_config is a configurable table which contains the details about the file we want to ingest and the table details. This stored proc iterates through each entry in this table and call the stored proc GENERATE_CLEAN_DDL.

Impacted tables:source_filelist_config

c.  CREATE_TABLES : This will create the tables from the ddl in ddl_details table.

Impacted tables:ddl_details

d.  RAW_DATA_INGESTION : This will ingest the data into snowflake based on the  source_filelist_config. An entry gets created in audit_table_inbound_feed with ingestion stats for each file ingested .

Impacted tables:audit_table_inbound_feed,source_filelist_config







