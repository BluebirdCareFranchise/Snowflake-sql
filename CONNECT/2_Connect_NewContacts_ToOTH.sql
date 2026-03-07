/*----------------------------------------------------------------------------------------
Module:   2_Connect_NewContacts_ToOTH
Purpose:  Send new HubSpot contacts to OTH via CSV export
Schedule: Runs after TASK_CONNECT_NEWCONTACTS (chained task)

Flow:
  REFINED.CONTACTS_NEWCUST (OTH filter) → CSV export → Update sent timestamp

Tables:
  REFINED Layer:
    - BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST (source - filtered for OTH)

Output:
  CSV files → @EXTERNAL_INTEGRATIONS.BBC_TO_OTH.STG_BBC_TO_OTH_HUBSPOT/
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
    v_filename STRING;
    v_pending NUMBER DEFAULT 0;
    v_sent NUMBER DEFAULT 0;
    result_message STRING;
BEGIN

    SELECT COUNT(*) INTO v_pending
    FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'One Touch Health'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    IF (v_pending = 0) THEN
        result_message := 'NEW→OTH: Pending=0 | No new contacts to send';
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOOTH', :result_message, 'SUCCESS');

        RETURN result_message;
    END IF;

    v_filename := 'Contacts_New_' || TO_CHAR(CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'YYYYMMDD_HH24MISS') || '.csv';

    EXECUTE IMMEDIATE '
        COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_OTH.STG_BBC_TO_OTH_HUBSPOT/' || v_filename || '
        FROM (
            SELECT 
                hs_object_id::STRING AS hs_object_id,
                firstname::STRING AS firstname,
                lastname::STRING AS lastname,
                salutation::STRING AS salutation,
                ''Active'' AS customer_status,
                customer_status_2::STRING AS customer_status_2,
                customer_status_other::STRING AS customer_status_other,
                date_of_birth::STRING AS date_of_birth,
                address::STRING AS address,
                city::STRING AS city,
                ZIP::STRING AS zip,
                country::STRING AS country,
                email::STRING AS email,
                mobilephone::STRING AS mobilephone,
                phone::STRING AS phone,
                contact_type::STRING AS contact_type,
                care_receiver_id::STRING AS care_receiver_id,
                family_member_ids::STRING AS family_member_ids,
                marital_status::STRING AS marital_status,
                office_location::STRING AS office_location,
                service_type::STRING AS service_type,
                service_type_2::STRING AS service_type_2,
                LASTMODIFIEDDATE::STRING AS lastmodifieddate
            FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
            WHERE ROSTER_SYSTEM = ''One Touch Health''
              AND CONTACT_TYPE IN (''Customer Family'', ''Care Receiver'')
              AND SENT_TIMESTAMP IS NULL
        )
        FILE_FORMAT = (TYPE = ''CSV'', FIELD_OPTIONALLY_ENCLOSED_BY = ''"'', COMPRESSION = ''NONE'')
        HEADER = TRUE SINGLE = TRUE';

    UPDATE BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
    WHERE ROSTER_SYSTEM = 'One Touch Health'
      AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    v_sent := SQLROWCOUNT;

    result_message := 'NEW→OTH: Pending=' || v_pending || ' | Sent=' || v_sent || ' → ' || v_filename;

    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
    (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
    VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOOTH', :result_message, :v_filename, 'SUCCESS');

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'NEW→OTH ERROR: ' || SQLERRM;
        
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
        (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        VALUES (CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, 'TASK_CONNECT_SENDTOOTH', :result_message, 'FAILURE');
             
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Chained after TASK_CONNECT_NEWCONTACTS
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_SENDTOOTH
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_CONNECT_NEWCONTACTS
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOOTH();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_SENDTOOTH RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_SENDTOOTH();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- Task logs
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS WHERE TASK_NAME = 'TASK_CONNECT_SENDTOOTH' ORDER BY LOG_TIMESTAMP DESC LIMIT 10;

-- Pending contacts for OTH
-- SELECT COUNT(*) AS pending FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST WHERE ROSTER_SYSTEM = 'One Touch Health' AND CONTACT_TYPE IN ('Customer Family', 'Care Receiver') AND SENT_TIMESTAMP IS NULL;

-- Recently sent
-- SELECT * FROM BBC_REFINED.HS_STRUTO.CONTACTS_NEWCUST WHERE ROSTER_SYSTEM = 'One Touch Health' ORDER BY SENT_TIMESTAMP DESC NULLS LAST LIMIT 20;

-- Stage files
-- LIST @EXTERNAL_INTEGRATIONS.BBC_TO_OTH.STG_BBC_TO_OTH_HUBSPOT/ PATTERN = '.*Contacts_New.*';
