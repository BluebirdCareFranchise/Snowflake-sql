/*----------------------------------------------------------------------------------------
Module:   CONNECT_1_GetNewContacts
Purpose:  Ingest new contact JSON files from HubSpot/Struto, map to roster systems
Schedule: Runs hourly Mon-Fri 08:55-17:55 UTC
Output:   BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST

Flow:
  Stage Files → Parse JSON → Map ROSTER_SYSTEM → Insert/Update contacts table
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
Procedure: Process new contact files from stage
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_NEWCONTACTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    processed_count INTEGER DEFAULT 0;
    result_message STRING;
    processed_files STRING DEFAULT '';
    inserted_count INTEGER DEFAULT 0;
    updated_count INTEGER DEFAULT 0;
BEGIN
    CREATE OR REPLACE TEMP TABLE temp_new_files AS
        SELECT DISTINCT SPLIT_PART(METADATA$FILENAME, '/', -1) AS file_name
        FROM @EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.STG_CONNECTHS_TO_BBC_CONTACTS
        WHERE METADATA$FILENAME LIKE 'ConnectHS_TO_BBC_Contacts/contacts_%'
        MINUS
        SELECT DISTINCT file_name
        FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files;

    SELECT COUNT(*) INTO processed_count FROM temp_new_files;
    SELECT LISTAGG(file_name, ', ') INTO processed_files FROM temp_new_files;

    IF (processed_count = 0) THEN
        result_message := 'NEW: From Hubspot: No new files to process.';
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, STATUS)
        SELECT CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP()), 'TASK_CONNECT_NEWCONTACTS', :result_message, 'SUCCESS';
        RETURN result_message;
    END IF;

    CREATE OR REPLACE TEMP TABLE temp_contacts_raw_json (json_data VARIANT);

    DECLARE
        file_cursor CURSOR FOR SELECT file_name FROM temp_new_files;
    BEGIN
        FOR rec IN file_cursor DO
            EXECUTE IMMEDIATE
                'COPY INTO temp_contacts_raw_json ' ||
                'FROM @EXTERNAL_INTEGRATIONS.CONNECTHS_TO_BBC.STG_CONNECTHS_TO_BBC_CONTACTS/ConnectHS_TO_BBC_Contacts/' || rec.file_name || ' ' ||
                'FILE_FORMAT = (FORMAT_NAME = BBC_SOURCE_RAW.HS_STRUTO.FILE_FORMAT_JSON) ' ||
                'ON_ERROR = CONTINUE';
        END FOR;
    END;

    DELETE FROM temp_contacts_raw_json WHERE json_data IS NULL OR json_data = PARSE_JSON('[]');

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
        CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP()) AS SENT_TIMESTAMP
    FROM temp_contacts_raw_json
    WHERE json_data IS NOT NULL;

    CREATE OR REPLACE TEMP TABLE delta_mapped AS
    WITH norm_alldata AS (
        SELECT 
            UPPER(REPLACE(REPLACE(TRIM(OFFICE_LOCATION), ' & ', ' AND '), ' ', '')) AS CLEAN_OFFICE,
            NULL AS ROSTER_SYSTEM, a.*
        FROM delta_connecths_contacts_newcust a
        UNION ALL
        SELECT 
            UPPER(REPLACE(REPLACE(TRIM(OFFICE_LOCATION), ' & ', ' AND '), ' ', '')) AS CLEAN_OFFICE,
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
    ),
    norm_hub AS (
        SELECT DISTINCT
            UPPER(REPLACE(REPLACE(TRIM(COMPANY_NAME), ' & ', ' AND '), ' ', '')) AS CLEAN_COMPANY,
            ROSTER_SYSTEM
        FROM (    
            SELECT 
                TRIM(REGEXP_REPLACE(
                    REGEXP_REPLACE(PROPERTY_NAME, 'Bluebird [Cc]are |BBC ', ''),
                    '[()-]', ''
                )) AS COMPANY_NAME, 
                PROPERTY_ROSTER_SYSTEM AS ROSTER_SYSTEM
            FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.VW_COMPANY
            WHERE IS_DELETED = 'FALSE' AND PROPERTY_TYPE = 'Franchisee'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY PROPERTY_NAME ORDER BY PROPERTY_NAME) = 1
        )
    )
    SELECT h.ROSTER_SYSTEM, a.* EXCLUDE (ROSTER_SYSTEM)
    FROM norm_alldata a
    LEFT JOIN norm_hub h ON a.CLEAN_OFFICE = h.CLEAN_COMPANY;

    UPDATE BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST tgt
    SET ROSTER_SYSTEM = src.ROSTER_SYSTEM,
        SENT_TIMESTAMP = CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP())
    FROM delta_mapped src
    WHERE tgt.HS_OBJECT_ID = src.HS_OBJECT_ID
      AND tgt.ROSTER_SYSTEM IS NULL
      AND src.ROSTER_SYSTEM IS NOT NULL;

    updated_count := SQLROWCOUNT;

    INSERT INTO BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST
    SELECT src.* EXCLUDE (CLEAN_OFFICE)
    FROM delta_mapped src
    LEFT JOIN BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST tgt ON src.HS_OBJECT_ID = tgt.HS_OBJECT_ID
    WHERE tgt.HS_OBJECT_ID IS NULL;

    inserted_count := SQLROWCOUNT;

    result_message := 'NEW: From Hubspot: Processed ' || processed_count || ' file(s), Inserted: ' || inserted_count || ', Updated: ' || updated_count || '. Files: [' || processed_files || ']';
    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
    SELECT CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP()), 'TASK_CONNECT_NEWCONTACTS', :result_message, :processed_files, 'SUCCESS';

    INSERT INTO BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files (file_name)
    SELECT file_name FROM temp_new_files;

    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := 'FAIL: From ConnectHS: ' || SQLERRM;
        INSERT INTO BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS (LOG_TIMESTAMP, TASK_NAME, RETURN_MESSAGE, PROCESSED_FILES, STATUS)
        SELECT CONVERT_TIMEZONE('UTC', 'Europe/London', CURRENT_TIMESTAMP()), 'TASK_CONNECT_NEWCONTACTS', :result_message, :processed_files, 'FAILURE';
        RETURN result_message;
END;
$$;

/*----------------------------------------------------------------------------------------
Tasks
----------------------------------------------------------------------------------------*/

CREATE OR REPLACE TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_NEWCONTACTS
    WAREHOUSE = REPORT_WH
    SCHEDULE = 'USING CRON 55 8-18 * * 1-5 UTC'
AS
    CALL BBC_SOURCE_RAW.HS_STRUTO.SP_CONNECT_NEWCONTACTS();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.HS_STRUTO.TASK_CONNECT_NEWCONTACTS RESUME;

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT * FROM BBC_SOURCE_RAW.HS_STRUTO.TASK_LOGS ORDER BY LOG_TIMESTAMP DESC;
-- SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.CONNECTHS_CONTACTS_NEWCUST ORDER BY LASTMODIFIEDDATE DESC;
-- SELECT file_name FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files ORDER BY FILE_NAME DESC;

/*----------------------------------------------------------------------------------------
Manual deletion
----------------------------------------------------------------------------------------*/
/*
DELETE FROM BBC_SOURCE_RAW.HS_STRUTO._contacts_processed_files 
WHERE file_name IN (
    'contacts_20260220_154002.json',
    'contacts_20260220_113003.json',
    'contacts_20260220_092003.json'
)


