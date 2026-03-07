/*----------------------------------------------------------------------------------------
Module:   2_Connect_NewContacts_ToPASS
Purpose:  Send new HubSpot contacts to PASS via shared table
Schedule: Runs after TASK_CONNECT_NEWCONTACTS (chained task)

Flow:
  REFINED.CONTACTS_NEWCUST (PASS filter) → Insert to share table → Update sent timestamp
  PASS processes → Returns acknowledgement → Captured in ACK table

Tables:
  REFINED Layer:
    - BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST (source - filtered for PASS)
    - BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK (acknowledgements)
  SEMANTIC Layer:
    - BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_NEWCUST_PASS_ACK (view for PowerBI)
  Share Out:
    - BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW (shared to PASS)
  Share In:
    - BLUEBIRD_DATA_LISTING.EXTRACT.ENQUIRIES_INBOUND (ACK from PASS)
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
One-Time Setup: ACK table and view
----------------------------------------------------------------------------------------*/
/*
CREATE TABLE IF NOT EXISTS BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK (
    HS_OBJECT_ID VARCHAR,
    CONTACT_TYPE VARCHAR,
    OFFICE_LOCATION VARCHAR,
    FIRSTNAME VARCHAR,
    LASTNAME VARCHAR,
    PROCESSED_TIMESTAMP TIMESTAMP_NTZ,
    FAILURE_REASON VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE VIEW BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_NEWCUST_PASS_ACK AS
SELECT * FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK;
*/

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
    v_pending NUMBER DEFAULT 0;
    v_sent NUMBER DEFAULT 0;
    result_message STRING;
BEGIN

    SELECT COUNT(*) INTO v_pending
    FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    IF (v_pending = 0) THEN
        result_message := 'NEW→PASS: Pending=0 | No new contacts to send';
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOPASS', :result_message, 'SUCCESS');

        RETURN result_message;
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
    FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    UPDATE BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    v_sent := SQLROWCOUNT;

    result_message := 'NEW→PASS: Pending=' || v_pending || ' | Sent=' || v_sent;

    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
    (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
    VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOPASS', :result_message, 'SUCCESS');

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'NEW→PASS ERROR: ' || SQLERRM;
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOPASS', :result_message, 'FAILURE');
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Get acknowledgements from PASS (refresh from share)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_PASS_ACK()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_before NUMBER DEFAULT 0;
    v_after NUMBER DEFAULT 0;
    v_new NUMBER DEFAULT 0;
    result_message STRING;
BEGIN

    SELECT COUNT(*) INTO v_before
    FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK;

    MERGE INTO BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK tgt
    USING (
        SELECT DISTINCT TO_VARCHAR(HS_OBJECT_ID) AS HS_OBJECT_ID, 
               CONTACT_TYPE, OFFICE_LOCATION, FIRSTNAME, LASTNAME,
               PROCESSED_TIMESTAMP, FAILURE_REASON
        FROM BLUEBIRD_DATA_LISTING.EXTRACT.ENQUIRIES_INBOUND
        WHERE YEAR(PROCESSED_TIMESTAMP) = YEAR(CURRENT_DATE())
    ) src
    ON tgt.HS_OBJECT_ID = src.HS_OBJECT_ID AND tgt.PROCESSED_TIMESTAMP = src.PROCESSED_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        HS_OBJECT_ID, CONTACT_TYPE, OFFICE_LOCATION, FIRSTNAME, LASTNAME,
        PROCESSED_TIMESTAMP, FAILURE_REASON
    ) VALUES (
        src.HS_OBJECT_ID, src.CONTACT_TYPE, src.OFFICE_LOCATION, src.FIRSTNAME, src.LASTNAME,
        src.PROCESSED_TIMESTAMP, src.FAILURE_REASON
    );

    SELECT COUNT(*) INTO v_after
    FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK;

    v_new := v_after - v_before;

    result_message := 'ACK←PASS: Before=' || v_before || ' | After=' || v_after || ' | New=' || v_new;

    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
    (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
    VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_PASS_ACK', :result_message, 'SUCCESS');

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'ACK←PASS ERROR: ' || SQLERRM;
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_PASS_ACK', :result_message, 'FAILURE');
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Tasks
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_SENDTOPASS
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_NEWCONTACTS
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOPASS();

CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_PASS_ACK
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 20 9-18 * * 1-5 UTC'
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_PASS_ACK();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- Run One-Time Setup first (ACK table + view)
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOPASS RESUME;
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_PASS_ACK RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOPASS();
-- CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_PASS_ACK();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- Task logs
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME LIKE '%PASS%' ORDER BY LOG_TIMESTAMP DESC LIMIT 10;

-- Pending contacts for PASS
-- SELECT COUNT(*) AS pending FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST WHERE ROSTER_SYSTEM = 'Everylife PASS' AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver') AND SENT_TIMESTAMP IS NULL;

-- Shared table (sent to PASS)
-- SELECT * FROM BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW ORDER BY PROCESSED_TIMESTAMP DESC LIMIT 20;

-- Acknowledgements from PASS
-- SELECT * FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK ORDER BY PROCESSED_TIMESTAMP DESC LIMIT 20;

-- ACK failures
-- SELECT * FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST_PASS_ACK WHERE FAILURE_REASON IS NOT NULL ORDER BY PROCESSED_TIMESTAMP DESC;
