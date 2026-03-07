/*----------------------------------------------------------------------------------------
Module:   4_Invoice_Semantic_PBI
Purpose:  Final PowerBI-ready tables from GOLD merge
Input:    BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE
Output:   
  - BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI (matched records)
  - BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI_EXCEPTION (unmatched records)
----------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE BBC_DWH_DEV.SEMANTICMODEL.SP_INVOICE_SEMANTIC_PBI()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    row_count_pbi INT;
    row_count_exc INT;
BEGIN
    USE DATABASE BBC_DWH_DEV;
    USE SCHEMA SEMANTICMODEL;

    /*====================================================================================
    1. HUBSPOT LOOKUP — CLEAN & UNIQUE
    ====================================================================================*/
    CREATE OR REPLACE TEMP TABLE TEMP_HUBSPOT_LOOKUP AS
    SELECT * EXCLUDE rn FROM (
        SELECT
            UPPER(REGEXP_REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(UPPER(TRIM(PROPERTY_NAME)), '&AMP;', 'AND'),
                                '&', 'AND'), 
                            'BLUEBIRD CARE', ''), 
                        'ONETOUCH', ''), 
                    '[^A-Z0-9]', '')) AS JOIN_KEY,
            TRIM(PROPERTY_NAME) AS HUBSPOT_OFFICE_NAME,
            TRIM(PROPERTY_POWER_BI_NAME) AS REPORT_NAME,
            PROPERTY_FRANCHISEE_1 AS FRANCHISE_OWNERS,
            TRIM(SPLIT_PART(PROPERTY_BDM, '-', 1)) AS BD_REGION,
            PROPERTY_STATUS AS STATUS,
            PROPERTY_ROSTER_SYSTEM AS HUBSPOT_ROSTER_SYSTEM,
            ROW_NUMBER() OVER (PARTITION BY JOIN_KEY ORDER BY PROPERTY_STATUS DESC) as rn
        FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY
        WHERE IS_DELETED = 'FALSE'
          AND PROPERTY_TYPE = 'Franchisee'
          AND PROPERTY_POWER_BI_REPORT = 'Yes'
    ) WHERE rn = 1 AND JOIN_KEY IS NOT NULL AND JOIN_KEY != '';

    /*====================================================================================
    2. REVENUE DATA — APPLY SAME CLEANING + FILTERS
    ====================================================================================*/
    CREATE OR REPLACE TEMP TABLE TEMP_REVENUE_PREPPED AS
    SELECT
        TRIM(COMPANY_NAME) AS REVENUE_OFFICE_NAME,
        UPPER(REGEXP_REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(UPPER(TRIM(COMPANY_NAME)), '&AMP;', 'AND'),
                        '&', 'AND'), 
                    'BLUEBIRD CARE', ''), 
                'ONETOUCH', ''), 
            '[^A-Z0-9]', '')) AS JOIN_KEY,
        ROSTER_SYSTEM,
        INVOICE_YEAR,
        INVOICE_MONTH,
        CARE_TYPE,
        FUNDER_TYPE,
        CATEGORY_OF_CARE,
        TOT_AMT_INVOICED,
        TOT_SUM_MSF,
        TOT_SUM_HOURS,
        TOT_CNT_CUSTOMERS,
        TOT_CNT_CARERS,
        TOT_AMT_EXPENSES
    FROM BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE
    WHERE DATE_FROM_PARTS(INVOICE_YEAR, EXTRACT(MONTH FROM TO_DATE(INVOICE_MONTH, 'Mon')), 1) < DATE_TRUNC('month', CURRENT_DATE)
      AND UPPER(COMPANY_NAME) NOT LIKE '%TRAINING%';

    /*====================================================================================
    3. MASTER JOIN
    ====================================================================================*/
    CREATE OR REPLACE TEMP TABLE TEMP_MASTER_DATA AS
    SELECT
        r.REVENUE_OFFICE_NAME AS COMPANY_NAME,
        r.REVENUE_OFFICE_NAME AS SOURCE_OFFICE,
        h.HUBSPOT_OFFICE_NAME,
        h.REPORT_NAME,
        r.ROSTER_SYSTEM,
        r.INVOICE_YEAR,
        r.INVOICE_MONTH,
        r.CARE_TYPE,
        r.FUNDER_TYPE,
        r.CATEGORY_OF_CARE,
        (r.TOT_AMT_INVOICED + COALESCE(r.TOT_AMT_EXPENSES, 0)) AS TOT_AMT_INVOICED,
        r.TOT_SUM_MSF,
        r.TOT_SUM_HOURS,
        r.TOT_CNT_CUSTOMERS,
        r.TOT_CNT_CARERS,
        r.TOT_AMT_EXPENSES,
        h.FRANCHISE_OWNERS,
        h.BD_REGION,
        h.STATUS,
        h.HUBSPOT_ROSTER_SYSTEM
    FROM TEMP_REVENUE_PREPPED r
    LEFT JOIN TEMP_HUBSPOT_LOOKUP h ON r.JOIN_KEY = h.JOIN_KEY;

    /*====================================================================================
    4. FINAL DISTRIBUTION
    ====================================================================================*/
    
    -- 4a. Main PBI Table (Matched Records)
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI AS
    SELECT * EXCLUDE HUBSPOT_OFFICE_NAME
    FROM TEMP_MASTER_DATA
    WHERE NOT (REPORT_NAME IS NULL OR REPORT_NAME = '');

    row_count_pbi := SQLROWCOUNT;

    -- 4b. Exception Table (Unmatched Source Revenue ONLY)
    CREATE OR REPLACE TABLE BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI_EXCEPTION AS
    SELECT * EXCLUDE (HUBSPOT_OFFICE_NAME, REPORT_NAME)
    FROM TEMP_MASTER_DATA
    WHERE REPORT_NAME IS NULL OR REPORT_NAME = '';

    row_count_exc := SQLROWCOUNT;

    RETURN 'SUCCESS: PBI (' || row_count_pbi || ' rows) | Exceptions (' || row_count_exc || ' rows)';
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Runs AFTER Gold merge completes
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_INVOICE_SEMANTIC_PBI
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_INVOICE_ALL_MERGE
AS
    CALL BBC_DWH_DEV.SEMANTICMODEL.SP_INVOICE_SEMANTIC_PBI();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- DROP TASK BBC_DWH_DEV.SEMANTICMODEL.TASK_INVOICE_SEMANTIC_PBI RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_DWH_DEV.SEMANTICMODEL.SP_INVOICE_SEMANTIC_PBI();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
/*
-- SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI_EXCEPTION LIMIT 100;

SELECT ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, COUNT(*), SUM(TOT_AMT_INVOICED) 
FROM BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI 
WHERE INVOICE_YEAR = 2026
GROUP BY 1, 2, 3 ORDER BY 1, 2 DESC;

SELECT INVOICE_MONTH, ROSTER_SYSTEM, COMPANY_NAME, SUM(TOT_AMT_INVOICED), SUM(TOT_AMT_EXPENSES)
FROM BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI
WHERE INVOICE_YEAR = 2026
GROUP BY 1,2,3 ORDER BY 1,2 desc, 2 DESC;

SELECT ROSTER_SYSTEM, COMPANY_NAME, SUM(TOT_AMT_INVOICED) 
SELECT * FROM BBC_DWH_DEV.SEMANTICMODEL.REVENUE_DATA_PBI_EXCEPTION 
WHERE INVOICE_YEAR = 2026 --AND INVOICE_MONTH = 'Feb'
GROUP BY 1,2 ORDER BY 1 desc, 2 DESC;