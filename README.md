# This repository will help you ingest raw data into snowflake from csv files hosted in azure blob.

This framework will perform the following activites for ingesting data into snowflake from csv files in azure blob.

1.Schema inference of the csv files in azure blob using INFER_SCHEMA()

2.DDL creation based on the schema inference. If the columns names have spaces ,they are replaces with an underscore.The DDLs are stored in a config table

3.Table creation based on the DDL in #2

4.Data ingestion into snowflake tables from the files 




