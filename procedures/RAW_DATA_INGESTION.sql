CREATE OR REPLACE PROCEDURE CUSTOMERS.PUBLIC.RAW_DATA_INGESTION
(
    CONFIG_DATABASE_NAME STRING,
    CONFIG_SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {

        // Generate unique execution_id for this run
        var uuid = snowflake.execute({sqlText: "select uuid_string()"});
        uuid.next();
        var execution_id = uuid.getColumnValue(1);
        
        // CREATE THE AUDIT TABLE IF NOT CREATED
        var audit_table_creation = `
                create table if not exists ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.audit_table_inbound_feed(
                filename varchar,
                rows_loaded int,
                file_load_status varchar,
                insert_timestamp timestamp,
                update_timestamp timestamp,
                inserted_by varchar,
                updated_by varchar
                );
        `;
        snowflake.execute({sqlText: audit_table_creation});
        
        var results = [];
        
        // 1. Query config table
        var cfg_stmt = snowflake.createStatement({
            sqlText: `SELECT 
                        STAGE_NAME
                        ,FILE_PATH
                        ,FILE_FORMAT_FILELOAD
                        ,FILE_NAME
                        ,TARGET_DATABASE_NAME
                        ,TARGET_SCHEMA_NAME
                        ,TARGET_TABLE_NAME
                        ,CONFIG_DATABASE_NAME
                        ,CONFIG_SCHEMA_NAME
                        ,CONFIG_TABLE_NAME
                        ,DDL_GENERATE_FLAG
                        ,ACTIVE_FLAG
                        FROM ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.source_filelist_config`
        });
    
        var rs = cfg_stmt.execute();
    
        // 2. Iterate over rows
        while (rs.next()) 
        {
            // Check if the ddl_generate_flag and active_flag are active
            if (rs.getColumnValue(11) == 'Y' && rs.getColumnValue(12) == 'Y') 
            {
                    var STAGE_NAME = rs.getColumnValue(1); 
                    var FILE_PATH = rs.getColumnValue(2);
                    var FILE_FORMAT_FILELOAD = rs.getColumnValue(3);     
                    var FILE_NAME = rs.getColumnValue(4);     
                    var TARGET_DATABASE_NAME = rs.getColumnValue(5);     
                    var TARGET_SCHEMA_NAME = rs.getColumnValue(6);    
                    var TARGET_TABLE_NAME = rs.getColumnValue(7);     
                    var CONFIG_DATABASE_NAME = rs.getColumnValue(8);    
                    var CONFIG_SCHEMA_NAME = rs.getColumnValue(9);    
                    var CONFIG_TABLE_NAME = rs.getColumnValue(10);    
       
            
                    // Query for table column list 
                    var sql_get_table_cols = `
                      SELECT LISTAGG(column_name, ', ') 
                             WITHIN GROUP (ORDER BY ordinal_position) AS table_cols
                      FROM information_schema.columns
                      WHERE table_name = '` + TARGET_TABLE_NAME + `'
                        AND table_schema = '` + TARGET_SCHEMA_NAME + `'
                        AND table_catalog = '` + TARGET_DATABASE_NAME + `'
                        `;
                    
                    var stmt1 = snowflake.createStatement({sqlText: sql_get_table_cols});
                    var stmt1_rs1 = stmt1.execute();
                    stmt1_rs1.next();
                    var table_cols = stmt1_rs1.getColumnValue(1);
                    
                    
                    // Query for file column list 
                    var sql_get_file_cols = `
                      SELECT LISTAGG('$' || file_cols, ', ')
                      WITHIN GROUP (ORDER BY ordinal_position) 
                      from
                        (
                        SELECT row_number() over(order by ordinal_position)  AS file_cols,ordinal_position
                        FROM information_schema.columns
                        WHERE table_name = '` + TARGET_TABLE_NAME + `'
                        AND table_schema = '` + TARGET_SCHEMA_NAME + `'
                        AND table_catalog = '` + TARGET_DATABASE_NAME + `'
                        AND column_name NOT IN ('INGESTDATE','FILENAME','INSERT_TIMESTAMP','UPDATE_TIMESTAMP'))`;
                     
                    var stmt2 = snowflake.createStatement({sqlText: sql_get_file_cols});
                    var stmt2_rs1 = stmt2.execute();
                    stmt2_rs1.next();
                    var file_cols = stmt2_rs1.getColumnValue(1);
                    
                    var copy_sql = `
                        copy into ${TARGET_DATABASE_NAME}.${TARGET_SCHEMA_NAME}.${TARGET_TABLE_NAME}
                        (${table_cols})
                        from 
                        (SELECT 
                        ${file_cols},
                        METADATA$START_SCAN_TIME,
                        METADATA$FILENAME,
                        current_timestamp,
                        current_timestamp
                        FROM ${STAGE_NAME}/${FILE_PATH}/)
                        PATTERN='${FILE_NAME}*.csv'
                        file_format = ${FILE_FORMAT_FILELOAD}
                        on_error = 'ABORT_STATEMENT'
                        force = true;
                    `;
            

                    var copy_sql_exec = snowflake.createStatement({sqlText: copy_sql});
                    var res_copy = copy_sql_exec.execute();
                    res_copy.next();

                    var loaded_file = res_copy.getColumnValue(1);   // file name
                    var load_status = res_copy.getColumnValue(2);  // load status  (2rd column in COPY INTO result set)
                    var rows_loaded = res_copy.getColumnValue(4);  // rows loaded (4th column in COPY INTO result set)

                    // AUDIT TABLE ENTRY
                    var audit_table_insert = `
                    INSERT INTO ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.audit_table_inbound_feed(
                        filename ,
                        rows_loaded ,
                        file_load_status ,
                        insert_timestamp ,
                        update_timestamp ,
                        inserted_by,
                        updated_by
                    )
                    VALUES
                    (
                        '${loaded_file}',
                        '${rows_loaded}',
                        '${load_status}',
                        current_timestamp,
                        current_timestamp,
                        current_user,
                        current_user
                    );
            `;
                    snowflake.execute({sqlText: audit_table_insert});

                    snowflake.log("INFO", "Step 4 : Data load complete for table :" + TARGET_TABLE_NAME , {stage: STAGE_NAME,file: FILE_NAME, table: TARGET_TABLE_NAME, execution_id: execution_id});
            
                   
             
                }
            }
            

        return "Step 4 : COPY INTO completed. " + results.join("; ");
    } catch (err) {
        snowflake.log("ERROR", "Step 4 : COPY INTO failed: " + err.message, {stage: STAGE_NAME, file: FILE_NAME, table: TARGET_TABLE_NAME, execution_id: execution_id});
        throw "Failed to load file from blob. Error: " + err.message;
    }
