/*-----------------------------------------------------------------------------------------------------
Module : CONNECTHS_2_SEND_UPD_PASS
Purpose: 
-----------------------------------------------------------------------------------------------------*/
USE SCHEMA BBC_SOURCE_RAW.HS_STRUTO;
        
CREATE OR REPLACE PROCEDURE SP_CONNECTHS_UPD_PASS()
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
    -- 2️⃣ Insert new rows based on LAST_UPDATE_TIMESTAMP
    ----------------------------------------------------------------
    INSERT INTO BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS (
        HS_OBJECT_ID,
        OFFICE_LOCATION,
        FIRSTNAME,
        SURNAME,
        CUSTOMER_STATUS,
        CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER,
        ADDRESS,
        CITY,
        ZIP,
        COUNTRY,
        EMAIL,
        MOBILEPHONE,
        PHONE,
        CONTACT_TYPE,
        LAST_UPDATE_TIMESTAMP,
        SENT_TO_HS_TIMESTAMP
    )
    SELECT
        s.HS_OBJECT_ID,
        s.OFFICE_LOCATION,
        s.FIRSTNAME,
        s.SURNAME,
        
        CASE 
            WHEN s.CUSTOMER_STATUS = 'ACTIVE' THEN 'Active'
            WHEN s.CUSTOMER_STATUS = 'INACTIVE' THEN 'Inactive'
            WHEN s.CUSTOMER_STATUS = 'FINISHED' THEN 'Finished'
            WHEN s.CUSTOMER_STATUS = 'ASSESSMENT COMPLETED' THEN 'Active'
            ELSE s.CUSTOMER_STATUS
        END AS CUSTOMER_STATUS,
     
        CASE 
            WHEN s.CUSTOMER_STATUS_2 = 'Hospital COVID-19 suspected' THEN 'Covid-19'
            WHEN s.CUSTOMER_STATUS_2 = 'Deceased COVID-19 suspected' THEN 'Deceased'
            WHEN s.CUSTOMER_STATUS_2 = 'Other care service' THEN 'Alternative provider'
            ELSE s.CUSTOMER_STATUS_2
        END AS CUSTOMER_STATUS_2,
    
        s.CUSTOMER_STATUS_OTHER,
        s.ADDRESS,
        s.CITY,
        s.ZIP,
        s.COUNTRY,
        s.EMAIL,
        s.MOBILEPHONE,
        s.PHONE,
        s.CONTACT_TYPE,
        s.LAST_UPDATE_TIMESTAMP,
        NULL  -- SENT_TO_HS_TIMESTAMP (to be set later)
    FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT s
    WHERE s.LAST_UPDATE_TIMESTAMP > COALESCE(
        (SELECT MAX(LAST_UPDATE_TIMESTAMP)
         FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS),
        '1900-01-01'::TIMESTAMP_NTZ
    );

    ----------------------------------------------------------------
    -- 3️⃣ Count unsent (new) rows
    ----------------------------------------------------------------
    v_new_rows := (
        SELECT COUNT(*)
        FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
        WHERE SENT_TO_HS_TIMESTAMP IS NULL
    );

    ----------------------------------------------------------------
    -- 4️⃣ Early exit if no new rows
    ----------------------------------------------------------------
    IF (v_new_rows = 0) THEN
        RETURN 'UPD: From PASS: NO NEW RECORDS SINCE LAST RUN';
    END IF;

    ----------------------------------------------------------------
    -- 5️⃣ Build JSON filename
    ----------------------------------------------------------------
    v_filename := 'CUSTOMER_UPDATE_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.json';

    ----------------------------------------------------------------
    -- 6️⃣ Export new rows as JSON to external stage
    ----------------------------------------------------------------
    sql_text := '
        COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/' || v_filename || '
        FROM (
            SELECT ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    ''HS_OBJECT_ID'', p.HS_OBJECT_ID,
                    ''FIRSTNAME'', p.FIRSTNAME,
                    ''SURNAME'', p.SURNAME,
                    ''CUSTOMER_STATUS'', p.CUSTOMER_STATUS,
                    ''CUSTOMER_STATUS_2'', p.CUSTOMER_STATUS_2,
                    ''CUSTOMER_STATUS_OTHER'', p.CUSTOMER_STATUS_OTHER,
                    ''ADDRESS'', p.ADDRESS,
                    ''CITY'', p.CITY,
                    ''ZIP'', p.ZIP,
                    ''COUNTRY'', p.COUNTRY,
                    ''EMAIL'', p.EMAIL,
                    ''MOBILEPHONE'', p.MOBILEPHONE,
                    ''PHONE'', p.PHONE,
                    ''CONTACT_TYPE'', p.CONTACT_TYPE,
                    ''PORTAL_ID'', c.PORTAL_ID
                )
            ) AS json_array
            FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS p
            LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST c
                ON p.HS_OBJECT_ID = c.ID
            WHERE p.SENT_TO_HS_TIMESTAMP IS NULL
        )
        FILE_FORMAT = (TYPE = ''JSON'', COMPRESSION = ''NONE'')
        OVERWRITE = TRUE
        SINGLE = TRUE;
    ';
    EXECUTE IMMEDIATE sql_text;

    ----------------------------------------------------------------
    -- 7️⃣ Update SENT_TO_HS_TIMESTAMP
    ----------------------------------------------------------------
    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
        SET SENT_TO_HS_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
        WHERE SENT_TO_HS_TIMESTAMP IS NULL;

    ----------------------------------------------------------------
    -- 8️⃣ Log result (must use EXECUTE IMMEDIATE to include variable)
    ----------------------------------------------------------------
    result_message := 'UPD: From PASS: Sent to HS ' || v_new_rows || ' rows in ' || v_filename;
    
    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''D60_CONNECT_UPD_PASS_SENDTOHS'',
             ''' || result_message || ''',
             ''SUCCESS''
         )';
    RETURN 'PASS Upd: Sent to HS ' || v_new_rows || ' rows in ' || v_filename;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK; 
        result_message := 'UPD: From PASS: Sent to HS ' || SQLERRM;
        result_message := REPLACE(result_message, '''', '''''');

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''D60_CONNECT_UPD_PASS_SENDTOHS'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';
        --RETURN result_message;
END;
$$;

CREATE OR REPLACE TASK D60_CONNECT_UPD_PASS_SENDTOHS
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 25 8-18 * * 1-5 UTC'  -- Runs at xx:25, Mon–Fri UTC
AS
    CALL SP_CONNECTHS_UPD_PASS();

--ALTER TASK D60_CONNECT_UPD_PASS_SENDTOHS RESUME;

/*
SHOW TASKS;
*/

/*
FOR TESTING PURPOSES ONLY:
-------------------------
SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS ORDER BY LOG_TIMESTAMP DESC;

SELECT * FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT --ORDER BY LAST_UPDATE_TIMESTAMP DESC
    WHERE LAST_UPDATE_TIMESTAMP > COALESCE(
        (SELECT MAX(LAST_UPDATE_TIMESTAMP) FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS), '1900-01-01'::TIMESTAMP_NTZ);

SELECT OBJECT_CONSTRUCT('CUSTOMER_STATUS_2', p.CUSTOMER_STATUS_2)
FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS p
WHERE p.CUSTOMER_STATUS_2 = 'Holiday';

DESC TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS WHERE HS_OBJECT_ID = 169035792943
2025-10-28 11:38:53.000
2025-10-28 11:10:25.000
UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
    SET SENT_TO_HS_TIMESTAMP = NULL
    WHERE HS_OBJECT_ID = 169035792943
    WHERE MONTH(SENT_TO_HS_TIMESTAMP) = 10 AND DAY(SENT_TO_HS_TIMESTAMP) > 10;


UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
    SET CUSTOMER_STATUS = 'Inactive' WHERE CUSTOMER_STATUS = 'INACTIVE';
UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
    --SET CUSTOMER_STATUS_2 = 'Covid-19' WHERE CUSTOMER_STATUS_2 = 'Hospital COVID-19 suspected'
    --SET CUSTOMER_STATUS_2 = 'Deceased' WHERE CUSTOMER_STATUS_2 = 'Deceased COVID-19 suspected'
    SET CUSTOMER_STATUS_2 = 'Alternative provider' WHERE CUSTOMER_STATUS_2 = 'Other care service';
*/

/* 
Day1 Setup:
----------
DROP TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS;
CREATE TABLE IF NOT EXISTS BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS (
        HS_OBJECT_ID STRING,
        OFFICE_LOCATION STRING,
        FIRSTNAME STRING,
        SURNAME STRING,
        CUSTOMER_STATUS STRING,
        CUSTOMER_STATUS_2 STRING,
        CUSTOMER_STATUS_OTHER STRING,
        ADDRESS STRING,
        CITY STRING,
        ZIP STRING,
        COUNTRY STRING,
        EMAIL STRING,
        MOBILEPHONE STRING,
        PHONE STRING,
        CONTACT_TYPE STRING,
        LAST_UPDATE_TIMESTAMP TIMESTAMP_NTZ,
        SENT_TO_HS_TIMESTAMP TIMESTAMP_NTZ
    );

/*****************DELETE

SELECT 
    s.CUSTOMER_STATUS_2,
    CASE 
        WHEN s.CUSTOMER_STATUS_2 = 'Hospital COVID-19 suspected' THEN 'Covid-19'
        WHEN s.CUSTOMER_STATUS_2 = 'Deceased COVID-19 suspected' THEN 'Deceased'
        WHEN s.CUSTOMER_STATUS_2 = 'Other care service' THEN 'Alternative provider'
        ELSE s.CUSTOMER_STATUS_2
    END AS derived_status_2
FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT s
WHERE s.CUSTOMER_STATUS_2 ILIKE '%Holiday%';
