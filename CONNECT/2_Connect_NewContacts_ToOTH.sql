/*----------------------------------------------------------------------------------------
Module:   CONNECT_2_SendToOTH
Purpose:  Send new contacts to OTH via CSV export
Depends:  CONNECT_1_GetNewContacts.sql (TASK_CONNECT_NEWCONTACTS)
Schedule: Runs after TASK_CONNECT_NEWCONTACTS

Flow:
  New Contacts → Filter OTH roster → Export CSV to stage → Update sent timestamp
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
Procedure: Send new contacts to OTH (CSV export)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOOTH()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_count NUMBER;
    copy_sql STRING;
    result_message STRING;
BEGIN
    ALTER SESSION SET TIMEZONE = 'Europe/London';

    SELECT COUNT(*) INTO v_count
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'One Touch Health'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    IF (v_count = 0) THEN
        RETURN 'NEW: To OTH: No new records since last run.';
    END IF;

    copy_sql := 'COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_OTH.STG_BBC_TO_OTH_HUBSPOT/Contacts_New_' ||
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.csv
        FROM (
            SELECT hs_object_id::STRING, firstname::STRING, lastname::STRING, salutation::STRING,
                ''Active'' AS customer_status, customer_status_2::STRING, customer_status_other::STRING,
                date_of_birth::STRING, address::STRING, city::STRING, ZIP::STRING, country::STRING,
                email::STRING, mobilephone::STRING, phone::STRING, contact_type::STRING,
                care_receiver_id::STRING, family_member_ids::STRING, marital_status::STRING,
                office_location::STRING, service_type::STRING, service_type_2::STRING,
                LASTMODIFIEDDATE::STRING
            FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
            WHERE ROSTER_SYSTEM = ''One Touch Health''
              AND CONTACT_TYPE IN (''Customer Family'', ''Care Receiver'')
              AND SENT_TIMESTAMP IS NULL
        )
        FILE_FORMAT = (TYPE = ''CSV'', FIELD_OPTIONALLY_ENCLOSED_BY = ''"'', COMPRESSION = ''NONE'')
        HEADER = TRUE SINGLE = TRUE';

    EXECUTE IMMEDIATE copy_sql;

    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    WHERE ROSTER_SYSTEM = 'One Touch Health'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    result_message := 'NEW: To OTH: Sent ' || v_count || ' records.';
    
    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''TASK_CONNECT_SENDTOOTH'',
             ''' || result_message || ''',
             ''SUCCESS''
         )';

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := REPLACE('NEW: To OTH: ' || SQLERRM, '''', '''''');
        
        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''TASK_CONNECT_SENDTOOTH'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Task
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOOTH
    WAREHOUSE = REPORT_WH
    AFTER BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_NEWCONTACTS
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOOTH();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOOTH RESUME;

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME = 'TASK_CONNECT_SENDTOOTH' ORDER BY LOG_TIMESTAMP DESC;
-- SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST WHERE ROSTER_SYSTEM = 'One Touch Health' ORDER BY SENT_TIMESTAMP DESC;
