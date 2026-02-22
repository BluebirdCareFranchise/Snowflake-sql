/*----------------------------------------------------------------------------------------
Module:   CONNECT_3_UpdatesFromPASS
Purpose:  Get customer status updates from PASS (via shared table) and send to HubSpot (JSON)
Depends:  PASS writes to shared table BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT
Schedule: Runs xx:25, Mon-Fri

Flow:
  PASS updates shared table → Timestamp-based delta detection → Export JSON → HubSpot
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
One-Time Setup: Table
----------------------------------------------------------------------------------------*/
/*
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
*/

/*----------------------------------------------------------------------------------------
Procedure: Process PASS updates and send to HubSpot (JSON export)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECTHS_UPD_PASS()
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
    ALTER SESSION SET TIMEZONE = 'Europe/London';

    INSERT INTO BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS (
        HS_OBJECT_ID, OFFICE_LOCATION, FIRSTNAME, SURNAME, CUSTOMER_STATUS, CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER, ADDRESS, CITY, ZIP, COUNTRY, EMAIL, MOBILEPHONE, PHONE,
        CONTACT_TYPE, LAST_UPDATE_TIMESTAMP, SENT_TO_HS_TIMESTAMP
    )
    SELECT
        s.HS_OBJECT_ID, s.OFFICE_LOCATION, s.FIRSTNAME, s.SURNAME,
        CASE 
            WHEN s.CUSTOMER_STATUS = 'ACTIVE' THEN 'Active'
            WHEN s.CUSTOMER_STATUS = 'INACTIVE' THEN 'Inactive'
            WHEN s.CUSTOMER_STATUS = 'FINISHED' THEN 'Finished'
            WHEN s.CUSTOMER_STATUS = 'ASSESSMENT COMPLETED' THEN 'Active'
            ELSE s.CUSTOMER_STATUS
        END,
        CASE 
            WHEN s.CUSTOMER_STATUS_2 = 'Hospital COVID-19 suspected' THEN 'Covid-19'
            WHEN s.CUSTOMER_STATUS_2 = 'Deceased COVID-19 suspected' THEN 'Deceased'
            WHEN s.CUSTOMER_STATUS_2 = 'Other care service' THEN 'Alternative provider'
            ELSE s.CUSTOMER_STATUS_2
        END,
        s.CUSTOMER_STATUS_OTHER, s.ADDRESS, s.CITY, s.ZIP, s.COUNTRY, s.EMAIL, s.MOBILEPHONE,
        s.PHONE, s.CONTACT_TYPE, s.LAST_UPDATE_TIMESTAMP, NULL
    FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT s
    WHERE s.LAST_UPDATE_TIMESTAMP > COALESCE(
        (SELECT MAX(LAST_UPDATE_TIMESTAMP) FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS),
        '1900-01-01'::TIMESTAMP_NTZ
    );

    v_new_rows := (SELECT COUNT(*) FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS WHERE SENT_TO_HS_TIMESTAMP IS NULL);

    IF (v_new_rows = 0) THEN
        RETURN 'UPD: From PASS: No new records since last run.';
    END IF;

    v_filename := 'CUSTOMER_UPDATE_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.json';

    sql_text := '
        COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_CONNECTHS.STG_BBC_TO_CONNECTHS/' || v_filename || '
        FROM (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                ''HS_OBJECT_ID'', p.HS_OBJECT_ID, ''FIRSTNAME'', p.FIRSTNAME, ''SURNAME'', p.SURNAME,
                ''CUSTOMER_STATUS'', p.CUSTOMER_STATUS, ''CUSTOMER_STATUS_2'', p.CUSTOMER_STATUS_2,
                ''CUSTOMER_STATUS_OTHER'', p.CUSTOMER_STATUS_OTHER, ''ADDRESS'', p.ADDRESS,
                ''CITY'', p.CITY, ''ZIP'', p.ZIP, ''COUNTRY'', p.COUNTRY, ''EMAIL'', p.EMAIL,
                ''MOBILEPHONE'', p.MOBILEPHONE, ''PHONE'', p.PHONE, ''CONTACT_TYPE'', p.CONTACT_TYPE,
                ''PORTAL_ID'', c.PORTAL_ID
            ))
            FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS p
            LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST c ON p.HS_OBJECT_ID = c.ID
            WHERE p.SENT_TO_HS_TIMESTAMP IS NULL
        )
        FILE_FORMAT = (TYPE = ''JSON'', COMPRESSION = ''NONE'')
        OVERWRITE = TRUE SINGLE = TRUE';
    EXECUTE IMMEDIATE sql_text;

    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS
    SET SENT_TO_HS_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    WHERE SENT_TO_HS_TIMESTAMP IS NULL;

    result_message := 'UPD: From PASS: Sent ' || v_new_rows || ' rows in ' || v_filename;
    
    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''TASK_CONNECT_UPD_PASS'',
             ''' || REPLACE(result_message, '''', '''''') || ''',
             ''SUCCESS''
         )';

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        result_message := REPLACE('UPD: From PASS: ' || SQLERRM, '''', '''''');
        
        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''TASK_CONNECT_UPD_PASS'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Task
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_UPD_PASS
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 25 8-18 * * 1-5 UTC'
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECTHS_UPD_PASS();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_UPD_PASS RESUME;

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT * FROM BLUEBIRD_DATA_LISTING.EXTRACT.CUSTOMERS_UPDATES_OUT ORDER BY LAST_UPDATE_TIMESTAMP DESC;
-- SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_UPDATES_PASS ORDER BY SENT_TO_HS_TIMESTAMP DESC;
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME LIKE '%PASS%' ORDER BY LOG_TIMESTAMP DESC;
