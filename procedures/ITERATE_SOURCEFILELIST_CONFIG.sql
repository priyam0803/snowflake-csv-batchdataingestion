CREATE OR REPLACE PROCEDURE CUSTOMERS.PUBLIC.ITERATE_SOURCEFILELIST_CONFIG
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
        
        var results = [];
        
        // 1. Query config table
        var cfg_stmt = snowflake.createStatement({
            sqlText: `SELECT 
                        STAGE_NAME
                        ,FILE_PATH
                        ,FILE_FORMAT
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
                    var FILE_FORMAT = rs.getColumnValue(3);     
                    var FILE_NAME = rs.getColumnValue(4);     
                    var TARGET_DATABASE_NAME = rs.getColumnValue(5);     
                    var TARGET_SCHEMA_NAME = rs.getColumnValue(6);    
                    var TARGET_TABLE_NAME = rs.getColumnValue(7);     
                    var CONFIG_DATABASE_NAME = rs.getColumnValue(8);    
                    var CONFIG_SCHEMA_NAME = rs.getColumnValue(9);    
                    var CONFIG_TABLE_NAME = rs.getColumnValue(10);    
            
                    // 3. Build dynamic CALL statement
                    var call_sp = `CALL ${TARGET_DATABASE_NAME}.${TARGET_SCHEMA_NAME}.GENERATE_CLEAN_DDL('${STAGE_NAME}', '${FILE_PATH}','${FILE_FORMAT}','${FILE_NAME}', '${CONFIG_DATABASE_NAME}','${CONFIG_SCHEMA_NAME}', '${CONFIG_TABLE_NAME}', '${TARGET_DATABASE_NAME}', '${TARGET_SCHEMA_NAME}', '${TARGET_TABLE_NAME}','${execution_id}')`;
            
                    var call_stmt = snowflake.createStatement({ sqlText: call_sp });
                    var call_rs = call_stmt.execute();
            
                        // 4. If inner proc returns something, capture it
                    if (call_rs.next()) 
                    {
                        results.push(call_rs.getColumnValue(1));
                    } else 
                    {
                        results.push(`Executed PROC GENERATE_CLEAN_DDL`);
                    }
            }
               
        }
    
        return results.join('\n');
    } catch (err) 
    {
    // Log error details
    snowflake.log('ERROR', 'Error occurred: ' + err.message, {execution_id: execution_id});
    
    // Optionally re-throw or return
    return 'Error: ' + err.message;
    }        
$$;

