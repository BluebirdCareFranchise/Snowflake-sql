/*----------------------------------------------------------------------------------------
Module:   CONNECT_2_SendToPASS
Purpose:  Send new contacts to PASS via shared table, receive acknowledgements
Depends:  CONNECT_1_GetNewContacts.sql (TASK_CONNECT_NEWCONTACTS)
Schedule: Runs after TASK_CONNECT_NEWCONTACTS

Flow:
  New Contacts → Filter PASS roster → Insert to share table → Update sent timestamp
  PASS processes → Returns acknowledgement → We capture in ACK table
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
Procedure: Send new contacts to PASS
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOPASS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    sent_count INT;
    result_message STRING;
BEGIN
    ALTER SESSION SET TIMEZONE = 'Europe/London';

    SELECT COUNT(*) INTO sent_count
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    IF (sent_count = 0) THEN
        RETURN 'NEW: To PASS: No new records since last run.';
    END IF;

    INSERT INTO BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW (
        HS_OBJECT_ID, CONTACT_TYPE, OFFICE_LOCATION, SERVICE_TYPE, SERVICE_TYPE_2,
        CUSTOMER_TYPE, CUSTOMER_STATUS, CUSTOMER_STATUS_2, CUSTOMER_STATUS_OTHER,
        DATE_OF_BIRTH, SALUTATION, FIRSTNAME, LASTNAME, ADDRESS, CITY, COUNTRY, ZIP,
        MOBILEPHONE, PHONE, EMAIL, MARITAL_STATUS, CARE_RECEIVER_ID, FAMILY_MEMBER_IDS,
        LAST_UPDATE_TIMESTAMP, PROCESSED_TIMESTAMP, FAILURE_REASON
    )
    SELECT 
        HS_OBJECT_ID, CONTACT_TYPE, OFFICE_LOCATION, SERVICE_TYPE, SERVICE_TYPE_2,
        CUSTOMER_TYPE, CUSTOMER_STATUS, CUSTOMER_STATUS_2, CUSTOMER_STATUS_OTHER,
        TRY_TO_DATE(DATE_OF_BIRTH, 'DD/MM/YYYY'), SALUTATION, FIRSTNAME, LASTNAME,
        ADDRESS, CITY, COUNTRY, ZIP, MOBILEPHONE, PHONE, EMAIL, MARITAL_STATUS,
        CARE_RECEIVER_ID, FAMILY_MEMBER_IDS, LASTMODIFIEDDATE, CURRENT_TIMESTAMP(), NULL
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    result_message := 'NEW: To PASS: Sent ' || sent_count || ' records.';
    
    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''TASK_CONNECT_SENDTOPASS'',
             ''' || result_message || ''',
             ''SUCCESS''
         )';

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := REPLACE('NEW: To PASS: ' || SQLERRM, '''', '''''');
        
        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''TASK_CONNECT_SENDTOPASS'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Get acknowledgements from PASS (for PBI reporting)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_PASS_ACK()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_NEWCUST_PASS_ACK AS
    SELECT DISTINCT t.* REPLACE (TO_VARCHAR(HS_OBJECT_ID) AS HS_OBJECT_ID)
    FROM BLUEBIRD_DATA_LISTING.EXTRACT.ENQUIRIES_INBOUND t
    WHERE YEAR(PROCESSED_TIMESTAMP) = YEAR(CURRENT_DATE());
    
    RETURN 'SUCCESS: PASS acknowledgements refreshed';
END;
$$;

/*----------------------------------------------------------------------------------------
Tasks
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOPASS
    WAREHOUSE = REPORT_WH
    AFTER BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_NEWCONTACTS
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOPASS();

CREATE OR REPLACE TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_PASS_ACK
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 20 9-18 * * 1-5 UTC'
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_PASS_ACK();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOPASS RESUME;
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_PASS_ACK RESUME;

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT * FROM BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW ORDER BY PROCESSED_TIMESTAMP DESC;
-- SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_NEWCUST_PASS_ACK ORDER BY PROCESSED_TIMESTAMP DESC;
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME LIKE '%PASS%' ORDER BY LOG_TIMESTAMP DESC;
