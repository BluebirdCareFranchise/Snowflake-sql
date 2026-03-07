/*----------------------------------------------------------------------------------------
Module:   1_Invoice_Raw_PASS
Purpose:  Clone tables from PASS data share and rebuild union views
Schedule: Runs 1st of every month at 07:00 UTC
          IMMUTABLE: Will NOT overwrite existing tables

Naming Convention:
  - Procedures: SP_PASS_<DATASET>_LOAD
  - Tables:     <DATASET>YYYY_M (e.g., SHARED_BBC_SUMMARY_INVOICE_2026_1)
  - Views:      VW_PASS_<DATASET>
  - Source:     BLUEBIRD_DATA_LISTING.EXTRACT.<DATASET>YYYY_M
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
Procedure: Clone invoice data from PASS share
Parameters:
  - DATASET: Dataset prefix (e.g., 'SHARED_BBC_SUMMARY_INVOICE_')
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.PASS.SP_PASS_INVOICE_LOAD(DATASET STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    year_month STRING;
    table_name STRING;
    full_table_name STRING;
    source_table STRING;
    table_exists INT;
    sql_view STRING;
    pattern STRING;
    row_count INT;
BEGIN
    -- Format: YYYY_M (no zero padding per PASS convention)
    year_month := TO_CHAR(DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE), 'YYYY') || '_' || 
                  CAST(MONTH(DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE)) AS STRING);
    table_name := DATASET || year_month;
    full_table_name := 'BBC_SOURCE_RAW.PASS.' || table_name;
    source_table := 'BLUEBIRD_DATA_LISTING.EXTRACT.' || table_name;
    
    -- Immutability: skip if table exists
    SELECT COUNT(*) INTO table_exists
    FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'PASS' AND TABLE_NAME = UPPER(:table_name);
    
    IF (table_exists > 0) THEN
        RETURN 'SKIPPED: ' || table_name || ' already exists';
    END IF;
    
    -- Clone from source share (via temp table to avoid cross-database clone issues)
    EXECUTE IMMEDIATE 'CREATE TABLE BBC_SOURCE_RAW.PASS.TEMP_PASS_CLONE AS SELECT * FROM ' || source_table;
    EXECUTE IMMEDIATE 'ALTER TABLE BBC_SOURCE_RAW.PASS.TEMP_PASS_CLONE RENAME TO BBC_SOURCE_RAW.PASS.' || table_name;
    
    SELECT COUNT(*) INTO row_count FROM IDENTIFIER(:full_table_name);
    
    -- Rebuild union view
    pattern := DATASET || '%';
    sql_view := (
        SELECT LISTAGG('SELECT * FROM BBC_SOURCE_RAW.PASS.' || TABLE_NAME, ' UNION ALL ') 
               WITHIN GROUP (ORDER BY TABLE_NAME)
        FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'PASS' 
          AND TABLE_NAME LIKE :pattern
          AND TABLE_NAME NOT LIKE '%HISTORY%'
          AND TABLE_NAME NOT LIKE '%TEMP%'
    );
    
    IF (sql_view IS NULL OR sql_view = '') THEN
        sql_view := 'SELECT * FROM BBC_SOURCE_RAW.PASS.' || table_name;
    END IF;
    
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW BBC_SOURCE_RAW.PASS.VW_PASS_' || DATASET || ' AS ' || sql_view;
    
    RETURN 'SUCCESS: ' || table_name || ' (' || row_count || ' rows)';
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Orchestrator - loads all PASS invoice datasets
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.PASS.SP_PASS_INVOICE_LOAD_ALL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    r1 STRING; 
    r2 STRING;
BEGIN
    CALL BBC_SOURCE_RAW.PASS.SP_PASS_INVOICE_LOAD('SHARED_BBC_SUMMARY_INVOICE_') INTO r1;
    CALL BBC_SOURCE_RAW.PASS.SP_PASS_INVOICE_LOAD('SHARED_BBC_SUMMARY_PERIOD_VISIT_') INTO r2;
    
    RETURN r1 || ' | ' || r2;
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Monthly PASS data clone (1st of month at 07:00 UTC)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_PASS_INVOICE_MONTHLY_LOAD
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_INVOICE_ROOT
AS
    CALL BBC_SOURCE_RAW.PASS.SP_PASS_INVOICE_LOAD_ALL();

/*----------------------------------------------------------------------------------------
Admin Commands
----------------------------------------------------------------------------------------*/
-- SHOW TASKS LIKE 'TASK_PASS%' IN SCHEMA BBC_SOURCE_RAW.PASS;
-- ALTER TASK BBC_SOURCE_RAW.PASS.TASK_PASS_INVOICE_MONTHLY_LOAD SUSPEND;
-- ALTER TASK BBC_SOURCE_RAW.PASS.TASK_PASS_INVOICE_MONTHLY_LOAD RESUME;
-- EXECUTE TASK BBC_SOURCE_RAW.PASS.TASK_PASS_INVOICE_MONTHLY_LOAD;

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT GET_DDL('VIEW', 'BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_');
-- SELECT GET_DDL('VIEW', 'BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_PERIOD_VISIT_');
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_;
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_PERIOD_VISIT_;

/*----------------------------------------------------------------------------------------
Historical Data Load (manual, one-time use)
----------------------------------------------------------------------------------------*/
/*
CREATE OR REPLACE TABLE BBC_SOURCE_RAW.PASS.SHARED_BBC_SUMMARY_INVOICE_HISTORY AS
    SELECT * FROM BLUEBIRD_DATA_LISTING.EXTRACT.AUG24_TO_JAN25_MMR_BYINV;
*/
SELECT OFFICE_NAME, INV_NO, CUSTOMER_ID, PERIOD_START, PERIOD_END, INVOICE_DATE, INVOICE_SENT_DATE, SENT_ON_ORIGINAL_DATE, SUM(TOTAL_INVOICED)
FROM BLUEBIRD_DATA_LISTING.EXTRACT.SHARED_BBC_SUMMARY_INVOICE_2026_1
WHERE OFFICE_NAME like '%Wirral%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

desc table  BLUEBIRD_DATA_LISTING.EXTRACT.SHARED_BBC_SUMMARY_INVOICE_2026_1