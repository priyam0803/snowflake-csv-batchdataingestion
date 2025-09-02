CREATE OR REPLACE PROCEDURE CUSTOMERS.PUBLIC.CREATE_TABLES
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
        var rs = snowflake.execute({sqlText: "select uuid_string()"});
        rs.next();
        var execution_id = rs.getColumnValue(1);
        
        var results = [];
    
        // Step 1: Select the DDL statements
        var ddl_create_active = snowflake.createStatement({
            sqlText: `SELECT ddl,database_name,schema_name,table_name 
            FROM ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.ddl_details
                      WHERE ACTIVE_FLAG='Y' and table_created_flag='N' ;`
        });
        var rs = ddl_create_active.execute();
    
        // Step 2: Iterate through rows
        while (rs.next()) {
            var ddl_sql = rs.getColumnValue(1);  // fetch the DDL string
            var database = rs.getColumnValue(2); 
            var schema = rs.getColumnValue(3); 
            var table = rs.getColumnValue(4); 
            
    
            // Step 3: Execute the DDL
            var execStmt = snowflake.createStatement({sqlText: ddl_sql});
            execStmt.execute();
    
            // Track what was executed
            results.push("Executed: " + ddl_sql);
            snowflake.log("info", "Step 3 : DDL created for table : " + table, {execution_id: execution_id});
    
            //Update the flag that the table was created 
            var ddl_create_update = snowflake.createStatement({
            sqlText: `UPDATE ${CONFIG_DATABASE_NAME}.${CONFIG_SCHEMA_NAME}.ddl_details
                      SET table_created_flag='Y',
                      UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
                      UPDATED_BY = CURRENT_USER
                      WHERE ACTIVE_FLAG='Y' AND
                      DATABASE_NAME='${database}'
                      AND SCHEMA_NAME='${schema}'
                      AND TABLE_NAME='${table}'
                      `});
            var rs_update = ddl_create_update.execute();
        }
    
        // If no data, print a message
        if (results.length === 0) {
            snowflake.log("info", "Step 3 : No rows found to execute DDL", {execution_id: execution_id});
            return 'Step 3 : No rows found to execute DDL';
        } else {
            return results.join('\n');
        }
            
    } catch (err) 
    {
    // Log error details
    snowflake.log('ERROR', 'Error occurred: ' + err.message, {execution_id: execution_id});
    
    // Optionally re-throw or return
    return 'Error: ' + err.message;
    }    
$$;
