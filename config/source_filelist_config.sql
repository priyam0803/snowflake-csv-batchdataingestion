USE SCHEMA customers.public;

create or replace table source_filelist_config
(
    id int,
    stage_type varchar() COMMENT 'Stage type e.g Azure Blob/AWS S3 etc',
    stage_name varchar() COMMENT 'Stage name',
    file_path varchar() COMMENT 'Path of the file',
    file_format varchar() COMMENT 'File format name for schema infer',
    file_format_fileload varchar() COMMENT 'File format name for file load',
    file_name varchar() COMMENT 'File  name',
    target_database_name varchar() COMMENT 'Target Database name',
    target_schema_name varchar() COMMENT 'Target Schema name',
    target_table_name varchar() COMMENT 'Target Table name',
    CONFIG_database_name varchar () COMMENT 'inferred schema database name',
    CONFIG_schema_name varchar () COMMENT 'inferred schema schema name',
    CONFIG_table_name varchar () COMMENT 'inferred schema table name',
    ddl_generate_flag varchar() ,
    active_flag varchar() ,
    inserted_timestamp timestamp,
    updated_timestamp timestamp,
    inserted_by varchar(),
    updated_by varchar()
        
)
;

insert into source_filelist_config values
(1,'Azure Blob','@azure_stage','customers','csv_format_inferschema','csv_format_fieload','Customers','CUSTOMERS','PUBLIC','CUSTOMERS','CUSTOMERS','PUBLIC','CUSTOMERS_INFERRED_SCHEMA','Y','Y',current_timestamp,current_timestamp,current_user,current_user);

insert into source_filelist_config values
(2,'Azure Blob','@azure_stage','sales','csv_format_inferschema','csv_format_fieload','retail_sales_dataset','CUSTOMERS','PUBLIC','SALES','CUSTOMERS','PUBLIC','SALES_INFERRED_SCHEMA','Y','Y',current_timestamp,current_timestamp,current_user,current_user);


