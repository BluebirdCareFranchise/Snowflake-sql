/*-----------------------------------------------------------------------------------------------------
Module : CONNECT_1_GET_NEWCUST
Purpose: Reads in new JSON files from Struto. Process and send to corresponding Roster Systems. 
        Recycle unprocessed records esp. when Office mapping in Company Hubspot is resolved
-----------------------------------------------------------------------------------------------------*/
USE SCHEMA BBC_SOURCE_RAW.HS_STRUTO;

/*================================================================================================*/
/*
--CREATE A VIEW to avoid issues with account permissions on underlying table where owner is the hubspot account 
    -- Drop the view if it exists
    DROP VIEW IF EXISTS EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY;
    -- Create the view
    CREATE VIEW EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY AS
        SELECT PROPERTY_NAME, PROPERTY_POWER_BI_NAME, PROPERTY_STATUS, PROPERTY_ROSTER_SYSTEM, PROPERTY_TYPE, IS_DELETED, PROPERTY_POWER_BI_REPORT
        FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY;
    -- Grant ownership
    GRANT OWNERSHIP ON VIEW EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY 
        TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS; 
*/
CREATE OR REPLACE PROCEDURE SP_COMPANY_HS_FT_UPDATES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    ----------------------------------------------------------------
    -- 0) Refresh COMPANY_INFO_HUBSPOT table from HubSpot
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.BBCF_HUBSPOT_COMPANY_INFO AS
    SELECT 
        TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(PROPERTY_NAME, '[()-]', ''), 'Bluebird Care ', ''), 'Bluebird care ', ''), 'Bluebird care', ''), 'bluebird care ', '')) AS COMPANY_NAME, 
        PROPERTY_POWER_BI_NAME AS BRANCH_NAME_PBI, 
        PROPERTY_STATUS AS STATUS, 
        PROPERTY_ROSTER_SYSTEM AS ROSTER_SYSTEM, 
        PROPERTY_TYPE
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PROPERTY_NAME ORDER BY PROPERTY_NAME) AS row_num
        FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY
        WHERE IS_DELETED = 'FALSE'
            AND PROPERTY_TYPE = 'Franchisee'
            AND PROPERTY_POWER_BI_REPORT = 'Yes'
    ) 
    QUALIFY row_num = 1;
END;
$$;
--SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.COMPANY_INFO_HUBSPOT;
CREATE OR REPLACE TASK D24H_COMPANY_HS_FT_UPDATES
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 0 8 * * 1-5 UTC'
AS
    CALL SP_COMPANY_HS_FT_UPDATES();
--ALTER TASK D24H_COMPANY_HS_FT_UPDATES  RESUME;
/*================================================================================================*/

CREATE OR REPLACE PROCEDURE SP_CONNECT_NEWCONTACTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    processed_count INTEGER;
    result_message  STRING;
    processed_files STRING;
BEGIN
    ----------------------------------------------------------------
    -- 1) Identify new files that haven't been processed yet
    ----------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE temp_new_files AS
        SELECT DISTINCT SPLIT_PART(METADATA$FILENAME,'/',-1) AS file_name
        FROM @EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.STG_CONNECTHS_TO_BBC_CONTACTS
        WHERE METADATA$FILENAME LIKE 'ConnectHS_TO_BBC_Contacts/contacts_%'
        MINUS
        SELECT DISTINCT file_name
        FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files;

    SELECT COUNT(*) INTO processed_count FROM temp_new_files;

    -- Build comma-separated file list (safe even if zero rows)
    SELECT LISTAGG(file_name, ', ')
    INTO processed_files
    FROM temp_new_files;

    ----------------------------------------------------------------
    -- 🛑 Early exit if no new files
    ----------------------------------------------------------------
    IF (processed_count = 0) THEN
        result_message := 'NEW: From Hubspot: No new files to process.';

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''D10_CONNECT_NEWCONTACTS'',
                 ''' || result_message || ''',
                 ''SUCCESS''
             )';

        RETURN result_message;
    END IF;

    ----------------------------------------------------------------
    -- 2) Load JSON data from new files
    ----------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE temp_contacts_raw_json (json_data VARIANT);

    IF (processed_count > 0) THEN
        DECLARE
            file_cursor CURSOR FOR SELECT file_name FROM temp_new_files;
        BEGIN
            FOR rec IN file_cursor DO
                EXECUTE IMMEDIATE
                    'COPY INTO temp_contacts_raw_json ' ||
                    'FROM @EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.STG_CONNECTHS_TO_BBC_CONTACTS/' || rec.file_name || ' ' ||
                    'FILE_FORMAT = (FORMAT_NAME = BBC_SOURCE_RAW.HS_STRUTO.FILE_FORMAT_JSON) ' ||
                    'ON_ERROR = CONTINUE';
            END FOR;
        END;

        DELETE FROM temp_contacts_raw_json
        WHERE json_data IS NULL OR json_data = PARSE_JSON('[]');
    END IF;

    ----------------------------------------------------------------
    -- 3) Parse JSON into structured contact records
    ----------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE delta_connecths_contacts_newcust AS
    SELECT 
        COALESCE(json_data:address::VARCHAR, '') AS ADDRESS,
        COALESCE(json_data:care_receiver_id::VARCHAR, '') AS CARE_RECEIVER_ID,
        COALESCE(json_data:city::VARCHAR, '') AS CITY,
        COALESCE(json_data:contact_type::VARCHAR, '') AS CONTACT_TYPE,
        COALESCE(json_data:country::VARCHAR, '') AS COUNTRY,
        TRY_TO_TIMESTAMP_NTZ(json_data:createdate::VARCHAR) AS CREATEDATE,
        COALESCE(json_data:customer_status::VARCHAR, '') AS CUSTOMER_STATUS,
        COALESCE(json_data:customer_status_2::VARCHAR, '') AS CUSTOMER_STATUS_2,
        COALESCE(json_data:customer_status_other::VARCHAR, '') AS CUSTOMER_STATUS_OTHER,
        COALESCE(json_data:customer_type::VARCHAR, '') AS CUSTOMER_TYPE,
        COALESCE(json_data:date_of_birth::VARCHAR, '') AS DATE_OF_BIRTH,
        COALESCE(json_data:email::VARCHAR, '') AS EMAIL,
        COALESCE(json_data:family_member_ids::VARCHAR, '') AS FAMILY_MEMBER_IDS,
        COALESCE(json_data:firstname::VARCHAR, '') AS FIRSTNAME,
        COALESCE(json_data:hs_object_id::VARCHAR, '') AS HS_OBJECT_ID,
        TRY_TO_TIMESTAMP_NTZ(json_data:hs_sa_first_engagement_date::VARCHAR) AS HS_SA_FIRST_ENGAGEMENT_DATE,
        COALESCE(json_data:hs_time_to_move_from_opportunity_to_customer::VARCHAR, '') AS HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER,
        COALESCE(json_data:id::VARCHAR, '') AS ID,
        TRY_TO_TIMESTAMP_NTZ(json_data:lastmodifieddate::VARCHAR) AS LASTMODIFIEDDATE,
        COALESCE(json_data:lastname::VARCHAR, '') AS LASTNAME,
        COALESCE(json_data:marital_status::VARCHAR, '') AS MARITAL_STATUS,
        COALESCE(json_data:mobilephone::VARCHAR, '') AS MOBILEPHONE,
        COALESCE(json_data:office_location::VARCHAR, '') AS OFFICE_LOCATION,
        COALESCE(json_data:phone::VARCHAR, '') AS PHONE,
        COALESCE(json_data:portal_id::VARCHAR, '') AS PORTAL_ID,
        COALESCE(json_data:relationship_to_customer__dependant_::VARCHAR, '') AS RELATIONSHIP_TO_CUSTOMER__DEPENDANT_,
        COALESCE(json_data:roster_account_number::VARCHAR, '') AS ROSTER_ACCOUNT_NUMBER,
        COALESCE(json_data:roster_provider::VARCHAR, '') AS ROSTER_PROVIDER,
        COALESCE(json_data:salutation::VARCHAR, '') AS SALUTATION,
        COALESCE(json_data:service_type::VARCHAR, '') AS SERVICE_TYPE,
        COALESCE(json_data:service_type_2::VARCHAR, '') AS SERVICE_TYPE_2,
        COALESCE(json_data:service_type_2_other::VARCHAR, '') AS SERVICE_TYPE_2_OTHER,
        COALESCE(json_data:source::VARCHAR, '') AS SOURCE,
        COALESCE(json_data:source_2::VARCHAR, '') AS SOURCE_2,
        COALESCE(json_data:state::VARCHAR, '') AS STATE,
        COALESCE(json_data:won_reason::VARCHAR, '') AS WON_REASON,
        COALESCE(json_data:zip::VARCHAR, '') AS ZIP,
        CONVERT_TIMEZONE('UTC','Europe/London', CURRENT_TIMESTAMP()) AS SENT_TIMESTAMP
    FROM temp_contacts_raw_json
    WHERE json_data IS NOT NULL;

    ----------------------------------------------------------------
    -- 4) Map ROSTER_SYSTEM
    ----------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE delta_mapped AS
    WITH norm_alldata AS (
        SELECT 
            UPPER(REPLACE(REPLACE(TRIM(OFFICE_LOCATION),' & ',' AND '),' ','')) AS CLEAN_OFFICE,
            a.*
        FROM (
            SELECT 
                NULL AS ROSTER_SYSTEM,
                ADDRESS, CARE_RECEIVER_ID, CITY, CONTACT_TYPE, COUNTRY, CREATEDATE,
                CUSTOMER_STATUS, CUSTOMER_STATUS_2, CUSTOMER_STATUS_OTHER, CUSTOMER_TYPE,
                DATE_OF_BIRTH, EMAIL, FAMILY_MEMBER_IDS, FIRSTNAME, HS_OBJECT_ID,
                HS_SA_FIRST_ENGAGEMENT_DATE, HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER, ID,
                LASTMODIFIEDDATE, LASTNAME, MARITAL_STATUS, MOBILEPHONE, OFFICE_LOCATION,
                PHONE, PORTAL_ID, RELATIONSHIP_TO_CUSTOMER__DEPENDANT_, ROSTER_ACCOUNT_NUMBER,
                ROSTER_PROVIDER, SALUTATION, SERVICE_TYPE, SERVICE_TYPE_2, SERVICE_TYPE_2_OTHER,
                SOURCE, SOURCE_2, STATE, WON_REASON, ZIP, SENT_TIMESTAMP
            FROM delta_connecths_contacts_newcust

            UNION ALL

            SELECT 
                ROSTER_SYSTEM, ADDRESS, CARE_RECEIVER_ID, CITY, CONTACT_TYPE, COUNTRY, CREATEDATE,
                CUSTOMER_STATUS, CUSTOMER_STATUS_2, CUSTOMER_STATUS_OTHER, CUSTOMER_TYPE, DATE_OF_BIRTH,
                EMAIL, FAMILY_MEMBER_IDS, FIRSTNAME, HS_OBJECT_ID, HS_SA_FIRST_ENGAGEMENT_DATE,
                HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER, ID, LASTMODIFIEDDATE, LASTNAME,
                MARITAL_STATUS, MOBILEPHONE, OFFICE_LOCATION, PHONE, PORTAL_ID,
                RELATIONSHIP_TO_CUSTOMER__DEPENDANT_, ROSTER_ACCOUNT_NUMBER, ROSTER_PROVIDER,
                SALUTATION, SERVICE_TYPE, SERVICE_TYPE_2, SERVICE_TYPE_2_OTHER,
                SOURCE, SOURCE_2, STATE, WON_REASON, ZIP, SENT_TIMESTAMP
            FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
            WHERE ROSTER_SYSTEM IS NULL
        ) a
    ),
    norm_hub AS (
        SELECT DISTINCT
            UPPER(REPLACE(REPLACE(TRIM(COMPANY_NAME),' & ',' AND '),' ','')) AS CLEAN_COMPANY,
            ROSTER_SYSTEM
 --       FROM BBC_SOURCE_RAW.HS_STRUTO.COMPANY_INFO_HUBSPOT
        FROM (    
        SELECT 
        TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(PROPERTY_NAME, '[()-]', ''), 'Bluebird Care ', ''), 'Bluebird care ', ''), 'Bluebird care', ''), 'bluebird care ', '')) AS COMPANY_NAME, 
        PROPERTY_ROSTER_SYSTEM AS ROSTER_SYSTEM
        FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PROPERTY_NAME ORDER BY PROPERTY_NAME) AS row_num
        FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY
        WHERE IS_DELETED = 'FALSE'
            AND PROPERTY_TYPE = 'Franchisee'
        ) 
        QUALIFY row_num = 1)
    )
    SELECT
        h.ROSTER_SYSTEM,
        a.ADDRESS, a.CARE_RECEIVER_ID, a.CITY, a.CONTACT_TYPE, a.COUNTRY, a.CREATEDATE,
        a.CUSTOMER_STATUS, a.CUSTOMER_STATUS_2, a.CUSTOMER_STATUS_OTHER, a.CUSTOMER_TYPE,
        a.DATE_OF_BIRTH, a.EMAIL, a.FAMILY_MEMBER_IDS, a.FIRSTNAME, a.HS_OBJECT_ID,
        a.HS_SA_FIRST_ENGAGEMENT_DATE, a.HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER, a.ID,
        a.LASTMODIFIEDDATE, a.LASTNAME, a.MARITAL_STATUS, a.MOBILEPHONE, a.OFFICE_LOCATION,
        a.PHONE, a.PORTAL_ID, a.RELATIONSHIP_TO_CUSTOMER__DEPENDANT_, a.ROSTER_ACCOUNT_NUMBER,
        a.ROSTER_PROVIDER, a.SALUTATION, a.SERVICE_TYPE, a.SERVICE_TYPE_2, a.SERVICE_TYPE_2_OTHER,
        a.SOURCE, a.SOURCE_2, a.STATE, a.WON_REASON, a.ZIP, a.SENT_TIMESTAMP
    FROM norm_alldata a
    LEFT JOIN norm_hub h
      ON a.CLEAN_OFFICE = h.CLEAN_COMPANY;

    ----------------------------------------------------------------
    -- 5) Update + Insert
    ----------------------------------------------------------------
    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST tgt
    SET ROSTER_SYSTEM = src.ROSTER_SYSTEM,
        SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC','Europe/London', CURRENT_TIMESTAMP())
    FROM delta_mapped src
    WHERE tgt.HS_OBJECT_ID = src.HS_OBJECT_ID
      AND tgt.ROSTER_SYSTEM IS NULL
      AND src.ROSTER_SYSTEM IS NOT NULL;

    INSERT INTO BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST (
        ROSTER_SYSTEM, ADDRESS, CARE_RECEIVER_ID, CITY, CONTACT_TYPE, COUNTRY, CREATEDATE,
        CUSTOMER_STATUS, CUSTOMER_STATUS_2, CUSTOMER_STATUS_OTHER, CUSTOMER_TYPE, DATE_OF_BIRTH,
        EMAIL, FAMILY_MEMBER_IDS, FIRSTNAME, HS_OBJECT_ID, HS_SA_FIRST_ENGAGEMENT_DATE,
        HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER, ID, LASTMODIFIEDDATE, LASTNAME,
        MARITAL_STATUS, MOBILEPHONE, OFFICE_LOCATION, PHONE, PORTAL_ID,
        RELATIONSHIP_TO_CUSTOMER__DEPENDANT_, ROSTER_ACCOUNT_NUMBER, ROSTER_PROVIDER,
        SALUTATION, SERVICE_TYPE, SERVICE_TYPE_2, SERVICE_TYPE_2_OTHER, SOURCE, SOURCE_2,
        STATE, WON_REASON, ZIP, SENT_TIMESTAMP
    )
    SELECT
        src.ROSTER_SYSTEM, src.ADDRESS, src.CARE_RECEIVER_ID, src.CITY, src.CONTACT_TYPE, src.COUNTRY, src.CREATEDATE,
        src.CUSTOMER_STATUS, src.CUSTOMER_STATUS_2, src.CUSTOMER_STATUS_OTHER, src.CUSTOMER_TYPE, src.DATE_OF_BIRTH,
        src.EMAIL, src.FAMILY_MEMBER_IDS, src.FIRSTNAME, src.HS_OBJECT_ID, src.HS_SA_FIRST_ENGAGEMENT_DATE,
        src.HS_TIME_TO_MOVE_FROM_OPPORTUNITY_TO_CUSTOMER, src.ID, src.LASTMODIFIEDDATE, src.LASTNAME,
        src.MARITAL_STATUS, src.MOBILEPHONE, src.OFFICE_LOCATION, src.PHONE, src.PORTAL_ID,
        src.RELATIONSHIP_TO_CUSTOMER__DEPENDANT_, src.ROSTER_ACCOUNT_NUMBER, src.ROSTER_PROVIDER,
        src.SALUTATION, src.SERVICE_TYPE, src.SERVICE_TYPE_2, src.SERVICE_TYPE_2_OTHER, src.SOURCE, src.SOURCE_2,
        src.STATE, src.WON_REASON, src.ZIP,
        NULL
    FROM delta_mapped src
    LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST tgt
      ON src.HS_OBJECT_ID = tgt.HS_OBJECT_ID
    WHERE tgt.HS_OBJECT_ID IS NULL;

    ----------------------------------------------------------------
    -- 6) Mark processed files
    ----------------------------------------------------------------
    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files (file_name)
    SELECT file_name FROM temp_new_files;

    ----------------------------------------------------------------
    -- 7) Log result
    ----------------------------------------------------------------
    result_message :=
        'NEW: From Hubspot: Processed ' || processed_count ||
        ' file(s). Files: [' || processed_files || ']';

    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''D10_CONNECT_NEWCONTACTS'',
             ''' || result_message || ''',
            ''' || processed_files || ''',
             ''SUCCESS''
         )';

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'FAIL: From ConnectHS: ' || SQLERRM;

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS
            (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''D10_CONNECT_NEWCONTACTS'',
                 ''' || result_message || ''',
                ''' || processed_files || ''',
               ''FAILURE''
             )';

        RETURN result_message;
END;
$$;


CREATE OR REPLACE TASK D10_CONNECT_NEWCONTACTS
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 55 8-18 * * 1-5 UTC'  -- Runs at xx:55, Mon–Fri, 08:55–17:55 UTC
AS
BEGIN
    CALL SP_CONNECT_NEWCONTACTS();
END;

-- TODO: lOOK FOR FILES ONLY 30 DAYS BACK? HOW TO HANDLE RERUNS? THE BELOW WILL PILEUP WITH FILENAMES
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files 
-- DELETE FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files WHERE FILE_NAME LIKE 'contacts_20250%'

--ALTER TASK D10_CONNECT_NEWCONTACTS  RESUME;
--GRANT USAGE ON PROCEDURE SP_CONNECT_NEWCONTACTS() TO ROLE ACCOUNTADMIN;
--GRANT USAGE ON PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_NEWCONTACTS() TO ROLE TASK_RUNNER_ROLE;

/*
SHOW TASKS;
SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS ORDER BY LOG_TIMESTAMP DESC;
TRUNCATE BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS; 
*/
/* CHECKS:
SHOW FILE FORMATS IN SCHEMA EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC;

---------
SELECT file_name FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files ORDER BY FILE_NAME DESC;
--DELETE FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files WHERE FILE_NAME LIKE 'contacts_2025090%';
DESC TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST ORDER BY LASTMODIFIEDDATE DESC;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST WHERE SENT_TIMESTAMP IS NULL;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST WHERE ROSTER_SYSTEM IS NOT NULL AND SENT_TIMESTAMP IS NULL;
--DELETE FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST WHERE SENT_TIMESTAMP IS NULL;
--DELETE FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST WHERE LASTMODIFIEDDATE IS NULL;
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST 
WHERE ROSTER_SYSTEM = 'Everylife PASS' AND CONTACT_TYPE IN ('Customer Family','Care Receiver') AND SENT_TIMESTAMP IS NULL; 

SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST ORDER BY SENT_TIMESTAMP DESC;
*/

/*------------------------------------------------------------------------------------------------
Send to PASS in shared table - only if there is data
*/
CREATE OR REPLACE PROCEDURE SP_CONNECT_SENDTOPASS()
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

    ----------------------------------------------------------------
    -- 1) Count how many rows were sent
    ----------------------------------------------------------------
    SELECT COUNT(*) INTO sent_count
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family','Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    ----------------------------------------------------------------
    -- 2) Early exit if no new rows
    ----------------------------------------------------------------
    IF (sent_count = 0) THEN
        RETURN 'NEW: To PASS: Early exit. No new records since last run.';
    END IF;

    ----------------------------------------------------------------
    -- 3) Insert new eligible contacts into PASS table
    ----------------------------------------------------------------
    INSERT INTO BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW (
        HS_OBJECT_ID,
        CONTACT_TYPE,
        OFFICE_LOCATION,
        SERVICE_TYPE,
        SERVICE_TYPE_2,
        CUSTOMER_TYPE,
        CUSTOMER_STATUS,
        CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER,
        DATE_OF_BIRTH,
        SALUTATION,
        FIRSTNAME,
        LASTNAME,
        ADDRESS,
        CITY,
        COUNTRY,
        ZIP,
        MOBILEPHONE,
        PHONE,
        EMAIL,
        MARITAL_STATUS,
        CARE_RECEIVER_ID,
        FAMILY_MEMBER_IDS,
        LAST_UPDATE_TIMESTAMP,
        PROCESSED_TIMESTAMP,
        FAILURE_REASON
    )
    SELECT 
        HS_OBJECT_ID,
        CONTACT_TYPE,
        OFFICE_LOCATION,
        SERVICE_TYPE,
        SERVICE_TYPE_2,
        CUSTOMER_TYPE,
        CUSTOMER_STATUS,
        CUSTOMER_STATUS_2,
        CUSTOMER_STATUS_OTHER,
        TRY_TO_DATE(DATE_OF_BIRTH, 'DD/MM/YYYY') AS DATE_OF_BIRTH,
        SALUTATION,
        FIRSTNAME,
        LASTNAME,
        ADDRESS,
        CITY,
        COUNTRY,
        ZIP,
        MOBILEPHONE,
        PHONE,
        EMAIL,
        MARITAL_STATUS,
        CARE_RECEIVER_ID,
        FAMILY_MEMBER_IDS,
        LASTMODIFIEDDATE AS LAST_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP() AS PROCESSED_TIMESTAMP,
        NULL AS FAILURE_REASON
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family','Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    ----------------------------------------------------------------
    -- 3) Update SENT_TIMESTAMP in source table
    ----------------------------------------------------------------
    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    WHERE ROSTER_SYSTEM = 'Everylife PASS'
      AND CONTACT_TYPE IN ('Customer Family','Care Receiver')
      AND SENT_TIMESTAMP IS NULL;

    ----------------------------------------------------------------
    -- 4) Log and Return message
    ----------------------------------------------------------------
    result_message := 'NEW: To PASS: Sent ' || sent_count || ' new records.';
    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''D60_CONNECT_NEW_SENDTOPASS'',
             ''' || result_message || ''',
             ''SUCCESS''
         )';

    RETURN 'To PASS: Sent ' || sent_count || ' new records.';

EXCEPTION
    WHEN OTHER THEN
        result_message := 'NEW: To PASS: ' || SQLERRM;
        result_message := REPLACE(result_message, '''', '''''');

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''D60_CONNECT_NEW_SENDTOPASS'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';

        RETURN result_message;
END;
$$;
/* CHECKS:
SELECT ROSTER_SYSTEM, CONTACT_TYPE, CREATEDATE, FIRSTNAME, HS_OBJECT_ID, LASTNAME, SENT_TIMESTAMP
FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST ORDER BY SENT_TIMESTAMP DESC;

SELECT HS_OBJECT_ID, OFFICE_LOCATION, LAST_UPDATE_TIMESTAMP, PROCESSED_TIMESTAMP 
FROM BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW
ORDER BY PROCESSED_TIMESTAMP DESC; 

SELECT HS_OBJECT_ID, OFFICE_LOCATION, LAST_UPDATE_TIMESTAMP, PROCESSED_TIMESTAMP, FAILURE_REASON, _DBT_REFRESH_TIMESTAMP 
FROM BLUEBIRD_DATA_LISTING.EXTRACT.ENQUIRIES_INBOUND ORDER BY PROCESSED_TIMESTAMP DESC ;
*/
CREATE OR REPLACE TASK D60_CONNECT_NEW_SENDTOPASS
WAREHOUSE = REPORT_WH
AFTER D10_CONNECT_NEWCONTACTS
AS
    CALL SP_CONNECT_SENDTOPASS();
/*
UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    SET SENT_TIMESTAMP = NULL
    WHERE ROSTER_SYSTEM = 'Everylife PASS' AND DATE(SENT_TIMESTAMP) = '10/27/2025'*/
--SELECT * FROM BBCF_SFSHARE_OUT.PASS.CONNECT_CONTACTS_NEW;
/*------------------------------------------------------------------------------------------------
Send to OTH as csv file - only if there is data
*/

CREATE OR REPLACE PROCEDURE SP_CONNECT_SENDTOOTH() 
RETURNS STRING 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS 
$$ 
DECLARE 
    filename STRING; 
    v_count  NUMBER; 
    copy_sql STRING;
    result_message STRING; 
BEGIN 
    ALTER SESSION SET TIMEZONE = 'Europe/London';

    ----------------------------------------------------------------
    -- 1) Count how many rows were sent
    ----------------------------------------------------------------
    SELECT COUNT(*) INTO v_count 
    FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST 
    WHERE ROSTER_SYSTEM = 'One Touch Health' 
      AND CONTACT_TYPE IN ('Customer Family','Care Receiver') 
      AND SENT_TIMESTAMP IS NULL; 

    ----------------------------------------------------------------
    -- 2) Early exit if no new rows
    ----------------------------------------------------------------
    IF (v_count = 0) THEN
        RETURN 'NEW: To OTH: Early exit. No new records since last run.';
    END IF;

    ----------------------------------------------------------------
    -- 3) Send eligible contacts to OTH
    ----------------------------------------------------------------
    copy_sql := CASE 
        WHEN v_count = 0 THEN 'SELECT 1'  -- No-op 
        ELSE  
            'COPY INTO @EXTERNAL_INTEGRATIONS.BBC_TO_OTH.STG_BBC_TO_OTH_HUBSPOT/Contacts_New_' || 
            TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.csv 
            FROM ( 
                SELECT                     
                    CAST(hs_object_id AS STRING) AS hs_object_id,
                    CAST(firstname AS STRING) AS firstname,
                    CAST(lastname AS STRING) AS lastname,
                    CAST(salutation AS STRING) AS salutation,
                    ''Active'' AS customer_status,
                    CAST(customer_status_2 AS STRING) AS customer_status_2,
                    CAST(customer_status_other AS STRING) AS customer_status_other,
                    CAST(date_of_birth AS STRING) AS date_of_birth,
                    CAST(address AS STRING) AS address,
                    CAST(city AS STRING) AS city,
                    CAST(ZIP AS STRING) AS ZIP,
                    CAST(country AS STRING) AS country,
                    CAST(email AS STRING) AS email,
                    CAST(mobilephone AS STRING) AS mobilephone,
                    CAST(phone AS STRING) AS phone,
                    CAST(contact_type AS STRING) AS contact_type,
                    CAST(care_receiver_id AS STRING) AS care_receiver_id,
                    CAST(family_member_ids AS STRING) AS family_member_ids,
                    CAST(marital_status AS STRING) AS marital_status,
                    CAST(office_location AS STRING) AS office_location,
                    CAST(service_type AS STRING) AS service_type,
                    CAST(service_type_2 AS STRING) AS service_type_2,
                    CAST(LASTMODIFIEDDATE AS STRING) AS LASTMODIFIEDDATE 
                FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST 
                WHERE ROSTER_SYSTEM = ''One Touch Health'' 
                  AND CONTACT_TYPE IN (''Customer Family'',''Care Receiver'') 
                  AND SENT_TIMESTAMP IS NULL 
            ) 
            FILE_FORMAT = (TYPE = ''CSV'', FIELD_OPTIONALLY_ENCLOSED_BY = ''"'', COMPRESSION = ''NONE'') 
            HEADER = TRUE 
            SINGLE = TRUE' 
    END; 

    -- Execute COPY safely 
/*    BEGIN */
        EXECUTE IMMEDIATE copy_sql;
/*    EXCEPTION 
        -- Log and Return
        WHEN OTHER THEN 
            result_message := 'NEW: To OTH: ' || SQLERRM;
            INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS (TASK_NAME, RETURN_MESSAGE, STATUS) VALUES ('D10_CONNECT_NEW_SENDTOOTH', result_message, 'FAILURE');
            RETURN 'FAIL: Send to OTH: ' || SQLERRM;
    END; */

    -- Update SENT_TIMESTAMP only after successful export 
    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST 
    SET SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    WHERE ROSTER_SYSTEM = 'One Touch Health' 
      AND CONTACT_TYPE IN ('Customer Family','Care Receiver') 
      AND SENT_TIMESTAMP IS NULL;

    ----------------------------------------------------------------
    -- 4) Log and Return message
    ----------------------------------------------------------------
    result_message := 'NEW: To OTH: Sent ' || v_count || ' new records.';

    EXECUTE IMMEDIATE
        'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
         (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
         VALUES (
             CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
             ''D10_CONNECT_NEW_SENDTOOTH'',
             ''' || result_message || ''',
             ''SUCCESS''
         )';

    RETURN 'To PASS: Sent ' || v_count || ' new records.';

EXCEPTION
    WHEN OTHER THEN
        result_message := 'NEW: To OTH: ' || SQLERRM;
        result_message := REPLACE(result_message, '''', '''''');

        EXECUTE IMMEDIATE
            'INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS 
             (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
             VALUES (
                 CONVERT_TIMEZONE(''UTC'', ''Europe/London'', CURRENT_TIMESTAMP()),
                 ''D10_CONNECT_NEW_SENDTOOTH'',
                 ''' || result_message || ''',
                 ''FAILURE''
             )';

        RETURN result_message;

END; 
$$;

CREATE OR REPLACE TASK D10_CONNECT_NEW_SENDTOOTH
WAREHOUSE = REPORT_WH
AFTER D10_CONNECT_NEWCONTACTS
AS
    CALL SP_CONNECT_SENDTOOTH();


/* Get acknowledge from PASS. Check if any fails */
CREATE OR REPLACE PROCEDURE SP_CONNECT_PASS_ACK()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_NEWCUST_PASS_ACK AS
        SELECT DISTINCT
            t.* REPLACE (TO_VARCHAR(HS_OBJECT_ID) AS HS_OBJECT_ID)
        FROM BLUEBIRD_DATA_LISTING.EXTRACT.ENQUIRIES_INBOUND t
        WHERE YEAR(PROCESSED_TIMESTAMP) = YEAR(CURRENT_DATE());
        -- Change this to Month, we don't need more than a month's to show in PBI

END;
$$;

CREATE OR REPLACE TASK D60_CONNECT_NEW_SENDTOPASS_ACK
WAREHOUSE = REPORT_WH
SCHEDULE = 'USING CRON 20 9-18 * * 1-5 UTC'  -- Runs at xx:59, Mon–Fri, 09:20–17:20 UTC
AS
    CALL SP_CONNECT_PASS_ACK();

/* END $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ */

/*
USE SCHEMA BBC_SOURCE_RAW.HS_STRUTO;
SHOW TASKS;
ALTER TASK D10_CONNECT_NEWCONTACTS SUSPEND; -- ROOT TASK
ALTER TASK D10_CONNECT_NEW_SENDTOOTH RESUME;
ALTER TASK D60_CONNECT_NEW_SENDTOPASS RESUME;
ALTER TASK D60_CONNECT_NEW_SENDTOPASS_ACK RESUME;
ALTER TASK D10_CONNECT_NEWCONTACTS RESUME;
SHOW TASKS;


CREATE OR REPLACE TABLE BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS (
    LOG_TIMESTAMP TIMESTAMP_LTZ DEFAULT CONVERT_TIMEZONE('Europe/London', SYSDATE())::TIMESTAMP_LTZ,
    TASK_NAME VARCHAR,
    RETURN_MESSAGE VARCHAR,
    STATUS VARCHAR
);
ALTER TABLE BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS ADD COLUMN PROCESSED_FILES VARCHAR;

SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE  -- This column is key for failure details!
FROM 
    TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP())
    ))
WHERE 
    NAME = 'D10_CONNECT_NEWCONTACTS'
ORDER BY 
    SCHEDULED_TIME DESC;
    */

/*
SHOW PROCEDURES LIKE 'SP_CONNECT_NEWCONTACTS' IN SCHEMA BBC_SOURCE_RAW.HS_STRUTO;
SHOW TASKS LIKE 'D10_CONNECT_NEWCONTACTS';
-- Look at the "owner" column

-- Grant permissions to that role (replace <OWNER_ROLE> with the actual role name)
GRANT USAGE ON DATABASE EXTERNAL_BBC_INTEGRATIONS TO ROLE <OWNER_ROLE>;
GRANT USAGE ON SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT TO ROLE <OWNER_ROLE>;
GRANT SELECT ON TABLE EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY TO ROLE <OWNER_ROLE>;

-- Also grant permissions on other objects the procedure needs
GRANT USAGE ON DATABASE EXTERNAL_INTEGRATIONS TO ROLE <OWNER_ROLE>;
GRANT USAGE ON SCHEMA EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC TO ROLE <OWNER_ROLE>;
GRANT READ ON STAGE EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.STG_CONNECTHS_TO_BBC TO ROLE <OWNER_ROLE>;
GRANT USAGE ON FILE FORMAT EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.file_format_json TO ROLE <OWNER_ROLE>;

-- Grant permissions on target tables
GRANT USAGE ON DATABASE BBC_DWH_DEV TO ROLE <OWNER_ROLE>;
GRANT USAGE ON SCHEMA BBC_DWH_DEV.SEMANTICMODEL TO ROLE <OWNER_ROLE>;
GRANT SELECT, INSERT, UPDATE ON TABLE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST TO ROLE <OWNER_ROLE>;

-- Grant on the external integration/share
GRANT IMPORTED PRIVILEGES ON DATABASE EXTERNAL_BBC_INTEGRATIONS TO ROLE ACCOUNTADMIN;

-- If the above doesn't work, check if it's a share
SHOW SHARES;
SHOW DATABASES LIKE 'EXTERNAL_BBC_INTEGRATIONS';

-- Check the database type
SELECT CURRENT_ROLE();
DESCRIBE DATABASE EXTERNAL_BBC_INTEGRATIONS;

-- Check what objects exist in that schema
SHOW TABLES IN SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT;

-- Check if COMPANY is a view or external table
DESCRIBE TABLE EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY;

-- Grant explicit permissions to ACCOUNTADMIN
GRANT USAGE ON DATABASE EXTERNAL_BBC_INTEGRATIONS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT TO ROLE ACCOUNTADMIN;

-- If COMPANY is a view
GRANT SELECT ON ALL VIEWS IN SCHEMA EXTERNAL_BBC_INTEGRATIONS.HUBSPOT TO ROLE ACCOUNTADMIN;

-- Check grants on the specific table
SHOW GRANTS ON TABLE EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY;


