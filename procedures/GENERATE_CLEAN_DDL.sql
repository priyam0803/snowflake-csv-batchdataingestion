----stored proc
CREATE OR REPLACE PROCEDURE CUSTOMERS.PUBLIC.GENERATE_CLEAN_DDL(
    STAGE_NAME STRING,
    FILE_PATH STRING,
    FILE_FORMAT_NAME STRING,
    FILE_NAME STRING,
    CONFIG_DATABASE_NAME STRING,
    CONFIG_SCHEMA_NAME STRING,
    CONFIG_TABLE_NAME STRING,
    TARGET_DATABASE_NAME STRING,
    TARGET_SCHEMA_NAME STRING,
    TARGET_TABLE_NAME STRING,
    EXECUTION_ID STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
  
    
    // Log procedure start
    snowflake.log('INFO', 'Procedure GENERATE_CLEAN_DDL() started for: '  + FILE_NAME, {execution_id: EXECUTION_ID});

    
        // CREATE THE DDL TABLE IF NOT CREATED
        var ddl_details = `
                create table if not exists ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.ddl_details(
                ddl varchar(),
                database_name varchar(),
                schema_name varchar(),
                table_name varchar(),
                insert_timestamp timestamp,
                update_timestamp timestamp,
                inserted_by varchar(),
                updated_by varchar(),
                active_flag varchar(),
                table_created_flag varchar()
                );
        `;
        snowflake.execute({sqlText: ddl_details});
    
        //INFER SCHEMA FROM THE FILE
        var infer_schema = `
            CREATE OR REPLACE TRANSIENT TABLE ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.${CONFIG_TABLE_NAME} AS
            SELECT * FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '${STAGE_NAME}/${FILE_PATH}/${FILE_NAME}.csv',
                    FILE_FORMAT => '${FILE_FORMAT_NAME}',
                    IGNORE_CASE=>TRUE
                )
            );
        `;
        snowflake.execute({sqlText: infer_schema});
        snowflake.log("info", "Step 1 : Schema inferred for file: " + FILE_NAME, {execution_id: EXECUTION_ID});
    
        // SOFT DELETE ANY EXISTING ENTRIES
        var ddl_sql_soft_delete = `
            UPDATE ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.ddl_details
            SET ACTIVE_FLAG = 'N',
                UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
                UPDATED_BY = CURRENT_USER
            WHERE DATABASE_NAME='${TARGET_DATABASE_NAME}'
            and SCHEMA_NAME ='${TARGET_SCHEMA_NAME}'
            and TABLE_NAME='${TARGET_TABLE_NAME}'
        `;
    
        snowflake.execute({sqlText: ddl_sql_soft_delete});
    
            //DDL CREATION
            var ddl_sql = `       
            INSERT INTO ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.ddl_details
            SELECT 
                'CREATE OR REPLACE TABLE ' || '` + TARGET_DATABASE_NAME + `' ||  '.' || '` + TARGET_SCHEMA_NAME + `' ||  '.' || '` + TARGET_TABLE_NAME + `' || ' (' ||
                LISTAGG(REGEXP_REPLACE(REGEXP_REPLACE(column_name, '[// //-]', '_'),'[()]','') || ' ' || type, ', ') 
                    WITHIN GROUP (ORDER BY order_id) ||
                ', ingestdate DATE' ||
                ', filename VARCHAR()' ||
                ', insert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP' ||
                ', update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP' 
                || ');' AS ddl,
                '${TARGET_DATABASE_NAME}'  as database_name,
                '${TARGET_SCHEMA_NAME}'  as schema_name,
                '${TARGET_TABLE_NAME}'  as table_name,
                 current_timestamp,
                 current_timestamp,
                 current_user,
                 current_user,
                 'Y',
                 'N'
            FROM ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.${CONFIG_TABLE_NAME};
        `;
        snowflake.execute({sqlText: ddl_sql});
        snowflake.log("info", "Step 2 : DDL generated for table " + TARGET_TABLE_NAME, {execution_id: EXECUTION_ID});
        
        return 'Step 2 : DDL Successfully generated for :'+ TARGET_TABLE_NAME

    } catch (err) 
    {
    // Log error details
    snowflake.log('ERROR', 'Error occurred: ' + err.message, {execution_id: EXECUTION_ID});
    
    // Optionally re-throw or return
    return 'Error: ' + err.message;
    }
$$;

--Calling stored procedure
CALL GENERATE_CLEAN_DDL(
    '@azure_stage',--Azure Stage name
    'csv_format',--FIle format
    'Customers.csv',--File name
    'CUSTOMERS',--inferred schema database name
    'PUBLIC',--inferred schema schema name
    'CUSTOMERS_INFERRED_SCHEMA',--inferred schema table name
    'CUSTOMERS',-- Target Database name
    'PUBLIC',--Target Schema name
    'CUSTOMERS'--Target table name
);
