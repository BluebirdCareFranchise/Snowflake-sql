/*-----------------------------------------------------------------------------------------------------
Module : CONNECTHS_2_SEND_UPD_OTH
Purpose: 
-----------------------------------------------------------------------------------------------------*/
USE SCHEMA BBC_SOURCE_RAW.HS_STRUTO;
/*
-- VV: CHECK the first few lines of the staged file
LIST @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS;
-- then pick one file and peek inside
SELECT *
FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS/CONTACTS_20250905_200001.csv
(FILE_FORMAT => PIPE_CSV_SAFE)
LIMIT 5;

-- get DDL and file format from one file
COPY INTO "EXTERNAL_INTEGRATIONS"."OTH_TO_BBC"."test223" 
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
	FROM '@"EXTERNAL_INTEGRATIONS"."OTH_TO_BBC"."STG_OTH_TO_BBC_CONNECT_STATUS"') 
FILES = ('CONTACTS_20250820_180902.csv') 
FILE_FORMAT = '"EXTERNAL_INTEGRATIONS"."OTH_TO_BBC"."temp_file_format_2025-08-21T21:23:08.694Z"' 
ON_ERROR=ABORT_STATEMENT 

-- Target table DDL, all Varchars
CREATE OR REPLACE TABLE CONTACT_STATUS_UPDATES_OTH_STG (
    HS_OBJECT_ID VARCHAR, FIRSTNAME VARCHAR, SURNAME VARCHAR, CUSTOMER_STATUS VARCHAR, CUSTOMER_STATUS_2 VARCHAR, CUSTOMER_STATUS_OTHER VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, ZIP VARCHAR, COUNTRY VARCHAR, EMAIL VARCHAR, MOBILEPHONE VARCHAR, PHONE VARCHAR, CONTACT_TYPE VARCHAR
);
DESC TABLE CONTACT_STATUS_UPDATES_OTH_STG;

CREATE OR REPLACE TABLE CONTACT_STATUS_UPDATES_OTH (
    HS_OBJECT_ID VARCHAR, FIRSTNAME VARCHAR, SURNAME VARCHAR, CUSTOMER_STATUS VARCHAR, CUSTOMER_STATUS_2 VARCHAR, CUSTOMER_STATUS_OTHER VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, ZIP VARCHAR, COUNTRY VARCHAR, EMAIL VARCHAR, MOBILEPHONE VARCHAR, PHONE VARCHAR, CONTACT_TYPE VARCHAR, SENT_TIMESTAMP TIMESTAMP_NTZ
);

-- File Format
CREATE OR REPLACE FILE FORMAT PIPE_CSV_SAFE
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  EMPTY_FIELD_AS_NULL = FALSE
  TRIM_SPACE = FALSE;
SHOW FILE FORMATS LIKE 'PIPE_CSV_FORMAT';
*/

-- Drop pipe
DROP PIPE PIPE_CONNECTHS_UPDATES_OTH;
-- Pause the pipe first
ALTER PIPE PIPE_CONNECTHS_UPDATES_OTH SET PIPE_EXECUTION_PAUSED = TRUE;
-- Recreate the pipe
CREATE OR REPLACE PIPE PIPE_CONNECTHS_UPDATES_OTH
AUTO_INGEST = TRUE
AS
COPY INTO BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS
FILE_FORMAT = (FORMAT_NAME = 'PIPE_CSV_SAFE')
--FORCE = TRUE
;
CREATE OR REPLACE STREAM CONTACT_STATUS_UPDATES_OTH_STG_STREAM
ON TABLE BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG;
-- Resume the pipe
ALTER PIPE PIPE_CONNECTHS_UPDATES_OTH SET PIPE_EXECUTION_PAUSED = FALSE;
/*
-- Clear the list of already processed files
-- day1
ALTER PIPE PIPE_CONNECTHS_UPDATES_OTH REFRESH; 
-- Check if pipe is running and recent activity
SHOW PIPES LIKE 'PIPE_CONNECTHS_UPDATES_OTH';
SELECT SYSTEM$PIPE_STATUS('PIPE_CONNECTHS_UPDATES_OTH');
*/
-- View the files already processed by the Pipe
/*
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'CONTACT_STATUS_UPDATES_OTH', START_TIME => DATEADD(days, -14, CURRENT_TIMESTAMP()))) WHERE PIPE_NAME = 'PIPE_CONNECTHS_UPDATES_OTH' ORDER BY LAST_LOAD_TIME DESC;

SELECT * FROM CONTACT_STATUS_UPDATES_OTH;
SELECT DISTINCT * FROM CONTACT_STATUS_UPDATES_OTH_STG;
--day1 DELETES
DELETE FROM CONTACT_STATUS_UPDATES_OTH_STG; 
DELETE FROM CONTACT_STATUS_UPDATES_OTH;
DELETE FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH; 
*/
/*
1. Identify current stage and files
LIST @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS;
-- Example: copy files into /rerun/ folder with new names
COPY INTO CONTACT_STATUS_UPDATES_OTH
FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS/CONTACTS_20250908_100001.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
FORCE=TRUE;
*/

--CREATE OR REPLACE TABLE CONTACT_STATUS_UPDATES_OTH AS
--    SELECT DISTINCT * FROM CONTACT_STATUS_UPDATES_OTH;

--SELECT DISTINCT PORTAL_ID, ID FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST

CREATE OR REPLACE PROCEDURE SP_CONNECTHS_UPDATES_SEND_JSON()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_filename STRING;
    sql_text STRING;
    v_new_rows NUMBER;
    result_message STRING;
BEGIN
    ----------------------------------------------------------------
    -- 1️⃣ Set session timezone
    ----------------------------------------------------------------
    ALTER SESSION SET TIMEZONE = 'Europe/London';


    ----------------------------------------------------------------
    -- 2️⃣ Insert new rows using hash of FINAL mapped values
    ----------------------------------------------------------------
    INSERT INTO CONTACT_STATUS_UPDATES_OTH (
        HS_OBJECT_ID, FIRSTNAME, SURNAME, CUSTOMER_STATUS, CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER, ADDRESS, CITY, ZIP, COUNTRY,
        EMAIL, MOBILEPHONE, PHONE, CONTACT_TYPE, SENT_TIMESTAMP
    )
    SELECT
        s.HS_OBJECT_ID,
        s.FIRSTNAME,
        s.SURNAME,
        s.FINAL_STATUS,
        s.FINAL_STATUS2,
        s.CUSTOMER_STATUS_OTHER,
        s.ADDRESS,
        s.CITY,
        s.ZIP,
        s.COUNTRY,
        s.EMAIL,
        s.MOBILEPHONE,
        s.PHONE,
        s.CONTACT_TYPE,
        NULL AS SENT_TIMESTAMP
    FROM (

        /* ----------------------------------------------
           STAGING with FINAL STATUS & FINAL HASH
        ---------------------------------------------- */
        SELECT
            stg.HS_OBJECT_ID,
            stg.FIRSTNAME,
            stg.SURNAME,
            stg.CUSTOMER_STATUS_OTHER,
            stg.ADDRESS,
            stg.CITY,
            stg.ZIP,
            stg.COUNTRY,
            stg.EMAIL,
            stg.MOBILEPHONE,
            stg.PHONE,
            stg.CONTACT_TYPE,

            /* Final CUSTOMER_STATUS mapping */
            CASE 
                WHEN stg.CUSTOMER_STATUS = 'Active' THEN 'Active'
                WHEN stg.CUSTOMER_STATUS = 'Pending' THEN 'Inactive'
                WHEN stg.CUSTOMER_STATUS = 'Archived' THEN 'Finished'
                ELSE stg.CUSTOMER_STATUS
            END AS FINAL_STATUS,

            /* Final CUSTOMER_STATUS_2 mapping */
            COALESCE(map.HS_CUSTOMER_STATUS_2, stg.CUSTOMER_STATUS_2) AS FINAL_STATUS2

        FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG stg
        LEFT JOIN BBC_SOURCE_RAW.ONETOUCH.OTH_MAPPING_CUSTOMER_STATUS_2 map
            ON stg.CUSTOMER_STATUS_2 = map.OTH_CUSTOMERSTATUS
        WHERE stg.HS_OBJECT_ID IS NOT NULL
          AND stg.HS_OBJECT_ID <> ''
    ) s

    /* ----------------------------------------------
       Compare using hash of FINAL values
    ---------------------------------------------- */
    LEFT JOIN (
        SELECT
            MD5(
                COALESCE(TO_VARCHAR(t.HS_OBJECT_ID), '') ||
                COALESCE(t.FIRSTNAME, '') ||
                COALESCE(t.SURNAME, '') ||
                COALESCE(t.CUSTOMER_STATUS, '') ||
                COALESCE(t.CUSTOMER_STATUS_2, '') ||
                COALESCE(t.CUSTOMER_STATUS_OTHER, '') ||
                COALESCE(t.ADDRESS, '') ||
                COALESCE(t.CITY, '') ||
                COALESCE(t.ZIP, '') ||
                COALESCE(t.COUNTRY, '') ||
                COALESCE(t.EMAIL, '') ||
                COALESCE(t.MOBILEPHONE, '') ||
                COALESCE(t.PHONE, '') ||
                COALESCE(t.CONTACT_TYPE, '')
            ) AS row_hash
        FROM CONTACT_STATUS_UPDATES_OTH t
    ) tgt
        ON tgt.row_hash =
            MD5(
                COALESCE(TO_VARCHAR(s.HS_OBJECT_ID), '') ||
                COALESCE(s.FIRSTNAME, '') ||
                COALESCE(s.SURNAME, '') ||
                COALESCE(s.FINAL_STATUS, '') ||
                COALESCE(s.FINAL_STATUS2, '') ||
                COALESCE(s.CUSTOMER_STATUS_OTHER, '') ||
                COALESCE(s.ADDRESS, '') ||
                COALESCE(s.CITY, '') ||
                COALESCE(s.ZIP, '') ||
                COALESCE(s.COUNTRY, '') ||
                COALESCE(s.EMAIL, '') ||
                COALESCE(s.MOBILEPHONE, '') ||
                COALESCE(s.PHONE, '') ||
                COALESCE(s.CONTACT_TYPE, '')
            )
    WHERE tgt.row_hash IS NULL;


    ----------------------------------------------------------------
    -- 3️⃣ Count unsent rows
    ----------------------------------------------------------------
    v_new_rows := (
        SELECT COUNT(*) FROM CONTACT_STATUS_UPDATES_OTH
        WHERE SENT_TIMESTAMP IS NULL
    );


    ----------------------------------------------------------------
    -- 4️⃣ Early exit
    ----------------------------------------------------------------
    IF (v_new_rows = 0) THEN
        RETURN 'UPD: From OTH: NO NEW RECORDS SINCE LAST RUN';
    END IF;


    ----------------------------------------------------------------
    -- 5️⃣ Build JSON filename
    ----------------------------------------------------------------
    v_filename := 'CUSTOMER_UPDATE_' || 
                  TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || 
                  '.json';


    ----------------------------------------------------------------
    -- 6️⃣ Export JSON
    ----------------------------------------------------------------
    sql_text := '
        COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/' || v_filename || '
        FROM (
            SELECT 
                ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        ''HS_OBJECT_ID'', u.HS_OBJECT_ID,
                        ''FIRSTNAME'', u.FIRSTNAME,
                        ''SURNAME'', u.SURNAME,
                        ''CUSTOMER_STATUS'', u.CUSTOMER_STATUS,
                        ''CUSTOMER_STATUS_2'', u.CUSTOMER_STATUS_2,
                        ''CUSTOMER_STATUS_OTHER'', u.CUSTOMER_STATUS_OTHER,
                        ''ADDRESS'', u.ADDRESS,
                        ''CITY'', u.CITY,
                        ''ZIP'', u.ZIP,
                        ''COUNTRY'', u.COUNTRY,
                        ''EMAIL'', u.EMAIL,
                        ''MOBILEPHONE'', u.MOBILEPHONE,
                        ''PHONE'', u.PHONE,
                        ''CONTACT_TYPE'', u.CONTACT_TYPE,
                        ''PORTAL_ID'', c.PORTAL_ID
                    )
                ) AS json_array
            FROM CONTACT_STATUS_UPDATES_OTH u
            LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST c
                ON u.HS_OBJECT_ID = c.ID
            WHERE u.SENT_TIMESTAMP IS NULL
        )
        FILE_FORMAT = (TYPE = ''JSON'', COMPRESSION = ''NONE'')
        OVERWRITE = TRUE
        SINGLE = TRUE;
    ';
    EXECUTE IMMEDIATE sql_text;

    ----------------------------------------------------------------
    -- 7️⃣ Mark exported rows as sent
    ----------------------------------------------------------------
    UPDATE CONTACT_STATUS_UPDATES_OTH
        SET SENT_TIMESTAMP = CONVERT_TIMEZONE('Europe/London','UTC',CURRENT_TIMESTAMP())
    WHERE SENT_TIMESTAMP IS NULL;


    ----------------------------------------------------------------
    -- 8️⃣ Refresh snapshot table
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH AS
        SELECT * FROM CONTACT_STATUS_UPDATES_OTH;


    ----------------------------------------------------------------
    -- 9️⃣ Log success
    ----------------------------------------------------------------
    result_message := 'UPD: From OTH: Sent to HS ' || v_new_rows || ' rows in ' || v_filename;

    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''Europe/London'',''UTC'',CURRENT_TIMESTAMP()),
             ''D60_CONNECT_UPD_OTH_2_SENDTOHS'',
             ''' || REPLACE(result_message, '''', '''''') || ''',
             ''SUCCESS''
         )';

    RETURN result_message;


EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;

        result_message := 'UPD: From OTH: Sent to HS ' || SQLERRM;
        result_message := REPLACE(result_message, '''', '''''');

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''Europe/London'',''UTC'',CURRENT_TIMESTAMP()),
                 ''D60_CONNECT_UPD_OTH_2_SENDTOHS'',
                 ''' || REPLACE(result_message, '''', '''''') || ''',
                 ''FAILURE''
             )';

        --RETURN result_message;
END;
$$;

CREATE OR REPLACE TASK D60_CONNECT_UPD_OTH_1_PIPE_RFR
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 03 8-17 * * 1-5 UTC'  -- Runs at xx:03, Mon–Fri, 08:55–17:55 UTC
AS
BEGIN
    ALTER PIPE PIPE_CONNECTHS_UPDATES_OTH REFRESH;
END;

CREATE OR REPLACE TASK D60_CONNECT_UPD_OTH_2_SENDTOHS
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 05 8-18 * * 1-5 UTC'  -- Runs at xx:05, Mon–Fri, 08:55–17:55 UTC
--WHEN SYSTEM$STREAM_HAS_DATA('CONTACT_STATUS_UPDATES_OTH_STG_STREAM')
AS
    CALL SP_CONNECTHS_UPDATES_SEND_JSON();

/*
SHOW TASKS;
ALTER TASK D60_CONNECT_UPD_OTH_1_PIPE_RFR SUSPEND;
ALTER TASK D60_CONNECT_UPD_OTH_1_PIPE_RFR RESUME;
ALTER TASK D60_CONNECT_UPD_OTH_2_SENDTOHS RESUME;

/*
FOR TESTING PURPOSES ONLY:
-------------------------
SELECT * FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT 
    WHERE s.LAST_UPDATE_TIMESTAMP > COALESCE(
        (SELECT MAX(LAST_UPDATE_TIMESTAMP)
         FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS),
        '1900-01-01'::TIMESTAMP_NTZ);

SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG;
        
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_STG
UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH
    SET SENT_TIMESTAMP = NULL
    WHERE HS_OBJECT_ID = '176626697392'
    WHERE MONTH(SENT_TIMESTAMP) = 10 AND DAY(SENT_TIMESTAMP) > 14;
*/

/* CHECKS:
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH ORDER BY SENT_TIMESTAMP DESC;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH WHERE FIRSTNAME = 'Maureen';
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST where hs_object_id = '154309221649';

--DELETE FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH where date(Sent_Timestamp) = '9/19/2025';
SELECT * FROM CONTACT_STATUS_UPDATES_OTH  where date(Sent_Timestamp) = '9/19/2025';
SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG ORDER BY HS_OBJECT_ID DESC;

DROP TABLE CONTACT_STATUS_UPDATES_OTH;


-- Check pipe ingestion history
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CONTACT_STATUS_UPDATES_OTH_STG',
        START_TIME => DATEADD('day', -1, CURRENT_TIMESTAMP),
        END_TIME   => CURRENT_TIMESTAMP
    )
)
ORDER BY LAST_LOAD_TIME DESC;
*/

/*
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE NAME = 'D60_CONNECT_UPD_TOHUBSPOT'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
-- Check last 10 executions
--SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'D60_CONNECT_UPD_TOHUBSPOT' ORDER BY SCHEDULED_TIME DESC LIMIT 10;
/*
INSERT INTO contact_status_updates 
VALUES 
('93380100000', 'Ealing', 'OTH', 'Pending', 'TEST Pending', 'Test', 'Test@outlook.com', 'Street', 'City', 'Region', 'Country', 'Postal', '999', '777', CURRENT_TIMESTAMP),
('1190451', 'Barnet', 'OTH', 'Inactive', 'TEST Inactive', 'Stella', 'stella.chinowawa@yahoo.co.uk', 'Street', 'City', 'Region', 'Country', 'Postal', '999', '777', CURRENT_TIMESTAMP),
('92329572690', 'Croydon', 'OTH', 'Active', 'TEST active', 'Mary', 'maryred5@yahoo.co.uk', 'Street', 'City', 'Region', 'Country', 'Postal', '999', '777', CURRENT_TIMESTAMP),
('93380101756', 'Bromley', 'OTH', 'Finished', 'TEST finished', 'Bob Tanaka', 'bob.tanaka@example.com', 'Street', 'City', 'Region', 'Country', 'Postal', '999', '777', CURRENT_TIMESTAMP);

select * from contact_status_updates;

SELECT TO_JSON(OBJECT_CONSTRUCT(*)) 
FROM my_table 
WHERE my_column = 'some_value';


COPY INTO '@EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/contact_updates_20250217_111213.json'
FROM (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM contact_status_updates) 
FILE_FORMAT = (TYPE = 'JSON', COMPRESSION = 'NONE') SINGLE = TRUE;


LIST @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS;


CREATE OR REPLACE PROCEDURE SP_CONNECTHS_UPDATES_SEND_JSON()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_filename VARCHAR(500);
    v_full_path VARCHAR(1000);
    v_sql VARCHAR(2000);
    v_record_count NUMBER;
BEGIN
    -- Check if table has any data
    SELECT COUNT(*) INTO v_record_count FROM CONTACT_STATUS_UPDATES_OTH;
    
    -- Exit early if no data
    CASE 
        WHEN v_record_count = 0 THEN
            RETURN 'CONNECTHS Updates: No data found in table. No file created.';
        ELSE
            NULL; -- Continue processing
    END CASE;
    
    -- Generate filename with current UTC datetime
    SELECT 'CUSTOMER_UPDATE_' || TO_CHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYYMMDD_HH24MISS') || '.json'
    INTO v_filename;
    
    -- Build full path
    v_full_path := '@EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/' || v_filename;
    
    -- Build dynamic COPY INTO statement
    v_sql := 'COPY INTO ' || v_full_path || ' ' ||
             'FROM (SELECT OBJECT_CONSTRUCT(*) FROM CONTACT_STATUS_UPDATES_OTH) ' ||
             'FILE_FORMAT = (TYPE = JSON, COMPRESSION = NONE) ' ||
             'SINGLE = TRUE ' ||
             'OVERWRITE = TRUE';
    
    -- Execute the dynamic SQL
    EXECUTE IMMEDIATE v_sql;
    
    -- Truncate the source table for fresh batch
    TRUNCATE TABLE CONTACT_STATUS_UPDATES_OTH;
    
    RETURN 'CONNECTHS Updates: JSON file created successfully as ' || v_filename || ' with ' || v_record_count || ' records';
END;
$$;

/*****************************************************************************************/
/*****************************************************************************************/
/*
ALTER SESSION SET TIMEZONE = 'UTC';
SELECT 
  PIPE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED,
  BYTES_INSERTED,
  FILES_INSERTED
FROM TABLE(
  INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    DATE_RANGE_END => CURRENT_TIMESTAMP(),
    PIPE_NAME => 'PIPE_CONNECTHS_UPDATES_OTH'
  )
)
ORDER BY START_TIME DESC;

SELECT
  FILE_NAME,
  STATUS,
  LAST_LOAD_TIME,
  ROW_COUNT,
  ERROR_COUNT,
  FIRST_ERROR_MESSAGE
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    --TABLE_NAME => 'CONTACT_STATUS_UPDATES_OTH_STG',
    START_TIME => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'PIPE_CONNECTHS_UPDATES_OTH'
  )
)
ORDER BY LAST_LOAD_TIME DESC;
