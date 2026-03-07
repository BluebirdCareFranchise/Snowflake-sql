/*----------------------------------------------------------------------------------------
Module:   3_Connect_UpdatesFrom_OTH
Purpose:  Sync contact status updates from OTH → HubSpot
Schedule: Mon-Fri, hourly 08:05-18:05 UTC

Flow:
  OTH CSV → Pipe → STG → REFINED (dedupe via hash) → JSON export → HubSpot

Tables:
  RAW Layer:
    - BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG (pipe landing)
    - BBC_SOURCE_RAW.ONETOUCH.OTH_MAPPING_CUSTOMER_STATUS_2 (lookup)
  REFINED Layer:
    - BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH (deduplicated target)
  SEMANTIC Layer:
    - BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH (view for PowerBI)

Output:
  JSON files → @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
One-Time Setup: Mapping table (load via Snowsight UI from CSV)
----------------------------------------------------------------------------------------*/
/*
CREATE TABLE BBC_SOURCE_RAW.ONETOUCH.OTH_MAPPING_CUSTOMER_STATUS_2 (
    OTH_CustomerStatus VARCHAR,
    HS_customer_status_2 VARCHAR
);
-- Upload CSV via Snowsight: Table → Load Data → Select CSV from desktop
*/

/*----------------------------------------------------------------------------------------
One-Time Setup: File Format, Tables, Pipe
----------------------------------------------------------------------------------------*/
/*
CREATE OR REPLACE FILE FORMAT BBC_SOURCE_RAW.HS_STRUTO.PIPE_CSV_SAFE
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = FALSE
    TRIM_SPACE = FALSE;

CREATE OR REPLACE TABLE BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG (
    HS_OBJECT_ID VARCHAR, FIRSTNAME VARCHAR, SURNAME VARCHAR, CUSTOMER_STATUS VARCHAR,
    CUSTOMER_STATUS_2 VARCHAR, CUSTOMER_STATUS_OTHER VARCHAR, ADDRESS VARCHAR, CITY VARCHAR,
    ZIP VARCHAR, COUNTRY VARCHAR, EMAIL VARCHAR, MOBILEPHONE VARCHAR, PHONE VARCHAR,
    CONTACT_TYPE VARCHAR
);

CREATE OR REPLACE PIPE BBC_SOURCE_RAW.HS_STRUTO.PIPE_CONNECTHS_UPDATES_OTH
    AUTO_INGEST = TRUE
AS
    COPY INTO BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG
    FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_CONNECT_STATUS
    FILE_FORMAT = (FORMAT_NAME = 'BBC_SOURCE_RAW.HS_STRUTO.PIPE_CSV_SAFE');
*/

/*----------------------------------------------------------------------------------------
One-Time Setup: REFINED schema and target table
----------------------------------------------------------------------------------------*/
/*
CREATE SCHEMA IF NOT EXISTS BBC_REFINED.HS_STRUTO;

CREATE TABLE IF NOT EXISTS BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH (
    HS_OBJECT_ID VARCHAR,
    FIRSTNAME VARCHAR,
    SURNAME VARCHAR,
    CUSTOMER_STATUS VARCHAR,
    CUSTOMER_STATUS_2 VARCHAR,
    CUSTOMER_STATUS_OTHER VARCHAR,
    ADDRESS VARCHAR,
    CITY VARCHAR,
    ZIP VARCHAR,
    COUNTRY VARCHAR,
    EMAIL VARCHAR,
    MOBILEPHONE VARCHAR,
    PHONE VARCHAR,
    CONTACT_TYPE VARCHAR,
    ROW_HASH VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
    SENT_TIMESTAMP TIMESTAMP_NTZ
);
*/

/*----------------------------------------------------------------------------------------
One-Time Setup: Semantic Layer View (for PowerBI)
----------------------------------------------------------------------------------------*/
/*
CREATE OR REPLACE VIEW BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_OTH AS
SELECT HS_OBJECT_ID, FIRSTNAME, SURNAME, CUSTOMER_STATUS, CUSTOMER_STATUS_2,
       CUSTOMER_STATUS_OTHER, ADDRESS, CITY, ZIP, COUNTRY, EMAIL, 
       MOBILEPHONE, PHONE, CONTACT_TYPE, SENT_TIMESTAMP
FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH;
*/

/*----------------------------------------------------------------------------------------
Procedure: Process OTH updates (STG → REFINED → JSON export)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_REFINED.HS_STRUTO.SP_CONNECT_UPDATES_OTH()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_filename STRING;
    v_stg_count NUMBER DEFAULT 0;
    v_stg_valid NUMBER DEFAULT 0;
    v_inserted NUMBER DEFAULT 0;
    v_dupes NUMBER DEFAULT 0;
    v_unsent NUMBER DEFAULT 0;
    v_sent NUMBER DEFAULT 0;
    result_message STRING;
BEGIN

    SELECT COUNT(*), COUNT(CASE WHEN HS_OBJECT_ID IS NOT NULL AND HS_OBJECT_ID <> '' THEN 1 END)
    INTO v_stg_count, v_stg_valid
    FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG;

    IF (v_stg_count = 0) THEN
        result_message := 'UPD←OTH: STG=0 | No data to process';
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_UPD_OTH', :result_message, 'SUCCESS');

        RETURN result_message;
    END IF;

    INSERT INTO BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH (
        HS_OBJECT_ID, FIRSTNAME, SURNAME, CUSTOMER_STATUS, CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER, ADDRESS, CITY, ZIP, COUNTRY,
        EMAIL, MOBILEPHONE, PHONE, CONTACT_TYPE, ROW_HASH, SENT_TIMESTAMP
    )
    SELECT
        s.HS_OBJECT_ID, s.FIRSTNAME, s.SURNAME, s.FINAL_STATUS, s.FINAL_STATUS2,
        s.CUSTOMER_STATUS_OTHER, s.ADDRESS, s.CITY, s.ZIP, s.COUNTRY,
        s.EMAIL, s.MOBILEPHONE, s.PHONE, s.CONTACT_TYPE, s.ROW_HASH, NULL
    FROM (
        SELECT
            stg.HS_OBJECT_ID, stg.FIRSTNAME, stg.SURNAME, stg.CUSTOMER_STATUS_OTHER,
            stg.ADDRESS, stg.CITY, stg.ZIP, stg.COUNTRY, stg.EMAIL, stg.MOBILEPHONE,
            stg.PHONE, stg.CONTACT_TYPE,
            CASE 
                WHEN stg.CUSTOMER_STATUS = 'Active' THEN 'Active'
                WHEN stg.CUSTOMER_STATUS = 'Pending' THEN 'Inactive'
                WHEN stg.CUSTOMER_STATUS = 'Archived' THEN 'Finished'
                ELSE stg.CUSTOMER_STATUS
            END AS FINAL_STATUS,
            COALESCE(map.HS_CUSTOMER_STATUS_2, stg.CUSTOMER_STATUS_2) AS FINAL_STATUS2,
            MD5(
                COALESCE(TO_VARCHAR(stg.HS_OBJECT_ID), '') || '|' ||
                COALESCE(stg.FIRSTNAME, '') || '|' ||
                COALESCE(stg.SURNAME, '') || '|' ||
                COALESCE(stg.CUSTOMER_STATUS, '') || '|' ||
                COALESCE(stg.CUSTOMER_STATUS_2, '') || '|' ||
                COALESCE(stg.CUSTOMER_STATUS_OTHER, '') || '|' ||
                COALESCE(stg.ADDRESS, '') || '|' ||
                COALESCE(stg.CITY, '') || '|' ||
                COALESCE(stg.ZIP, '') || '|' ||
                COALESCE(stg.COUNTRY, '') || '|' ||
                COALESCE(stg.EMAIL, '') || '|' ||
                COALESCE(stg.MOBILEPHONE, '') || '|' ||
                COALESCE(stg.PHONE, '') || '|' ||
                COALESCE(stg.CONTACT_TYPE, '')
            ) AS ROW_HASH
        FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG stg
        LEFT JOIN BBC_SOURCE_RAW.ONETOUCH.OTH_MAPPING_CUSTOMER_STATUS_2 map
            ON stg.CUSTOMER_STATUS_2 = map.OTH_CUSTOMERSTATUS
        WHERE stg.HS_OBJECT_ID IS NOT NULL AND stg.HS_OBJECT_ID <> ''
    ) s
    WHERE NOT EXISTS (
        SELECT 1 FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH tgt
        WHERE tgt.ROW_HASH = s.ROW_HASH
    );

    v_inserted := SQLROWCOUNT;
    v_dupes := v_stg_valid - v_inserted;

    TRUNCATE TABLE BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG;

    SELECT COUNT(*) INTO v_unsent
    FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH
    WHERE SENT_TIMESTAMP IS NULL;

    IF (v_unsent = 0) THEN
        result_message := 'UPD←OTH: STG=' || v_stg_count || ' (valid=' || v_stg_valid || ') | Inserted=' || v_inserted || ' | Dupes=' || v_dupes || ' | Sent=0';
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_UPD_OTH', :result_message, 'SUCCESS');

        RETURN result_message;
    END IF;

    v_filename := 'CUSTOMER_UPDATE_' || TO_CHAR(CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'YYYYMMDD_HH24MISS') || '.json';

    EXECUTE IMMEDIATE '
        COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/' || v_filename || '
        FROM (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
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
            ))
            FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH u
            LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST c ON u.HS_OBJECT_ID = c.ID
            WHERE u.SENT_TIMESTAMP IS NULL
              AND u.HS_OBJECT_ID IS NOT NULL AND u.HS_OBJECT_ID <> ''
        )
        FILE_FORMAT = (TYPE = ''JSON'', COMPRESSION = ''NONE'')
        OVERWRITE = TRUE SINGLE = TRUE';

    UPDATE BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
    WHERE SENT_TIMESTAMP IS NULL
      AND HS_OBJECT_ID IS NOT NULL AND HS_OBJECT_ID <> '';

    v_sent := SQLROWCOUNT;

    result_message := 'UPD←OTH: STG=' || v_stg_count || ' (valid=' || v_stg_valid || ') | Inserted=' || v_inserted || ' | Dupes=' || v_dupes || ' | Sent=' || v_sent || ' → ' || v_filename;

    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
    (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
    VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_UPD_OTH', :result_message, :v_filename, 'SUCCESS');

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'UPD←OTH ERROR: ' || SQLERRM;
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_UPD_OTH', :result_message, 'FAILURE');
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Tasks
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_OTH_PIPE_REFRESH
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 03 8-17 * * 1-5 UTC'
AS
    ALTER PIPE BBC_SOURCE_RAW.HS_STRUTO.PIPE_CONNECTHS_UPDATES_OTH REFRESH;

CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_UPD_OTH
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 05 8-18 * * 1-5 UTC'
AS
    CALL BBC_REFINED.HS_STRUTO.SP_CONNECT_UPDATES_OTH();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- Run One-Time Setup sections first (mapping table, schema, target table)
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_OTH_PIPE_REFRESH RESUME;
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_UPD_OTH RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_REFINED.HS_STRUTO.SP_CONNECT_UPDATES_OTH();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- Task logs
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME = 'TASK_CONNECT_UPD_OTH' ORDER BY LOG_TIMESTAMP DESC LIMIT 10;

-- Pipe status
-- SELECT SYSTEM$PIPE_STATUS('BBC_SOURCE_RAW.HS_STRUTO.PIPE_CONNECTHS_UPDATES_OTH');

-- STG (should be empty after successful run)
-- SELECT COUNT(*) AS stg_total, COUNT(CASE WHEN HS_OBJECT_ID IS NOT NULL AND HS_OBJECT_ID <> '' THEN 1 END) AS stg_valid FROM BBC_SOURCE_RAW.HS_STRUTO.CONTACT_STATUS_UPDATES_OTH_STG;

-- REFINED target
-- SELECT COUNT(*) AS total, SUM(CASE WHEN SENT_TIMESTAMP IS NULL THEN 1 ELSE 0 END) AS unsent FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH;
-- SELECT * FROM BBC_REFINED.HS_STRUTO.CONTACT_UPDATES_OTH ORDER BY CREATED_AT DESC LIMIT 20;

-- Mapping table
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.ONETOUCH.OTH_MAPPING_CUSTOMER_STATUS_2;
