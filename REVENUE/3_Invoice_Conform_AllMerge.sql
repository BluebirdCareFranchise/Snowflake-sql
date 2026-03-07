/*----------------------------------------------------------------------------------------
Module:   3_Invoice_Conform_AllMerge
Purpose:  Merge all SILVER sources into GOLD semantic model for PowerBI

Data Sources (5 Silver tables):
  1. PASS:       BBC_REFINED.PASS.PASS_INVOICE_SUMM
  2. OTH-UK:     BBC_REFINED.OTH.OTH_134_MMR_SUMM (≤ Dec 2025)
                 BBC_REFINED.OTH.OTH_134_INVOICE_SUMM (≥ Jan 2026)
  3. OTH-IRL:    BBC_REFINED.OTH.OTH_149_MMR_SUMM (≤ Dec 2025)
                 BBC_REFINED.OTH.OTH_149_INVOICE_SUMM (≥ Jan 2026)
  4. STAFFPLAN:  BBC_SOURCE_RAW.STAFFPLAN.STAFFPLAN_MMRFINANCE (static)
  5. WEBROSTER:  BBC_SOURCE_RAW.WEBROSTER_ONETOUCH.WEBROSTER_MMRFINANCE (static)

Output: BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE
----------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE BBC_CONFORMED.REVENUE.SP_INVOICE_ALL_MERGE()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    row_count INT DEFAULT 0;
    curr_month_total NUMBER DEFAULT 0;
    prev_month_total NUMBER DEFAULT 0;
    variance_pct NUMBER;
    variance_msg STRING DEFAULT '';
    process_date DATE;
    curr_year INT;
    curr_month INT;
BEGIN
    USE DATABASE BBC_DWH_DEV;
    USE SCHEMA SEMANTICMODEL;

    -- HubSpot company lookup
    CREATE OR REPLACE TEMPORARY TABLE TEMP_HUBSPOT_COMPANIES AS
    SELECT 
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(PROPERTY_NAME, 'Bluebird [Cc]are |BBC ', ''),
            '[()-]', ''
        )) AS COMPANY_NAME,
        PROPERTY_POWER_BI_NAME AS BRANCH_NAME_PBI
    FROM EXTERNAL_BBC_INTEGRATIONS.HUBSPOT.COMPANY
    WHERE IS_DELETED = 'FALSE' 
      AND PROPERTY_TYPE = 'Franchisee' 
      AND PROPERTY_POWER_BI_REPORT = 'Yes'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PROPERTY_NAME ORDER BY ID DESC) = 1;

    -- Final merge: UNION ALL Silver tables + HubSpot join
    CREATE OR REPLACE TABLE BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE AS
    WITH all_silver AS (
        -- 1. OTH-UK MMR Silver (≤ Dec 2025)
        SELECT COMPANY_NAME, ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, 
               CARE_TYPE, FUNDER_TYPE, CATEGORY_OF_CARE,
               TOT_AMT_INVOICED, TOT_SUM_MSF, TOT_SUM_HOURS, TOT_CNT_CUSTOMERS, TOT_CNT_CARERS,
               NULL AS TOT_AMT_EXPENSES
        FROM BBC_REFINED.OTH.OTH_134_MMR_SUMM
        WHERE INVOICE_YEAR < 2026

        UNION ALL

        -- 2. OTH-UK Invoice Silver (≥ Jan 2026)
        SELECT 
            BRANCHNAME AS COMPANY_NAME,
            ROSTER_SYSTEM,
            INVOICE_YEAR,
            TO_CHAR(DATE_FROM_PARTS(INVOICE_YEAR, INVOICE_MONTH, 1), 'MON') AS INVOICE_MONTH,
            COALESCE(FUNDER_TYPE, 'XXX') AS CARE_TYPE,
            COALESCE(FUNDER_TYPE, 'XXX') AS FUNDER_TYPE,
            CATEGORY_OF_CARE,
            TOT_AMT_INVOICED,
            NULL AS TOT_SUM_MSF,
            TOT_CNT_HOURS AS TOT_SUM_HOURS,
            NULL AS TOT_CNT_CUSTOMERS,
            NULL AS TOT_CNT_CARERS,
            TOT_AMT_EXPENSES
        FROM BBC_REFINED.OTH.OTH_134_INVOICE_SUMM
        WHERE INVOICE_YEAR >= 2026

        UNION ALL

        -- 3. OTH-IRL MMR Silver (≤ Dec 2025)
        SELECT COMPANY_NAME, ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, 
               CARE_TYPE, FUNDER_TYPE, CATEGORY_OF_CARE,
               TOT_AMT_INVOICED, TOT_SUM_MSF, TOT_SUM_HOURS, TOT_CNT_CUSTOMERS, TOT_CNT_CARERS,
               NULL AS TOT_AMT_EXPENSES
        FROM BBC_REFINED.OTH.OTH_149_MMR_SUMM
        WHERE INVOICE_YEAR < 2026

        UNION ALL

        -- 4. OTH-IRL Invoice Silver (≥ Jan 2026)
        SELECT 
            BRANCHNAME AS COMPANY_NAME,
            ROSTER_SYSTEM,
            INVOICE_YEAR,
            TO_CHAR(DATE_FROM_PARTS(INVOICE_YEAR, INVOICE_MONTH, 1), 'MON') AS INVOICE_MONTH,
            COALESCE(FUNDER_TYPE, 'XXX') AS CARE_TYPE,
            COALESCE(FUNDER_TYPE, 'XXX') AS FUNDER_TYPE,
            CATEGORY_OF_CARE,
            TOT_AMT_INVOICED,
            NULL AS TOT_SUM_MSF,
            TOT_CNT_HOURS AS TOT_SUM_HOURS,
            NULL AS TOT_CNT_CUSTOMERS,
            NULL AS TOT_CNT_CARERS,
            TOT_AMT_EXPENSES
        FROM BBC_REFINED.OTH.OTH_149_INVOICE_SUMM
        WHERE INVOICE_YEAR >= 2026

        UNION ALL

        -- 5. PASS Silver
        SELECT 
            COMPANY_NAME, ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, 
            CARE_TYPE, FUNDER_TYPE, 
            NULL AS CATEGORY_OF_CARE,
            TOT_AMT_INVOICED, TOT_SUM_MSF, TOT_SUM_HOURS, TOT_CNT_CUSTOMERS, TOT_CNT_CARERS,
            NULL AS TOT_AMT_EXPENSES
        FROM BBC_REFINED.PASS.PASS_INVOICE_SUMM

        UNION ALL

        -- 6. STAFFPLAN (static)
        SELECT 
            COMPANY_NAME, ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, 
            CARE_TYPE, FUNDER_TYPE, 
            NULL AS CATEGORY_OF_CARE,
            TOT_AMT_INVOICED, TOT_SUM_MSF, TOT_SUM_HOURS, TOT_CNT_CUSTOMERS, TOT_CNT_CARERS,
            NULL AS TOT_AMT_EXPENSES
        FROM BBC_SOURCE_RAW.STAFFPLAN.STAFFPLAN_MMRFINANCE

        UNION ALL

        -- 7. WEBROSTER (static)
        SELECT 
            COMPANY_NAME, ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, 
            CARE_TYPE, FUNDER_TYPE, 
            NULL AS CATEGORY_OF_CARE,
            TOT_AMT_INVOICED, TOT_SUM_MSF, TOT_SUM_HOURS, TOT_CNT_CUSTOMERS, TOT_CNT_CARERS,
            NULL AS TOT_AMT_EXPENSES
        FROM BBC_SOURCE_RAW.WEBROSTER_ONETOUCH.WEBROSTER_MMRFINANCE
    )
    SELECT
        hc.BRANCH_NAME_PBI,
        a.COMPANY_NAME,
        a.ROSTER_SYSTEM,
        a.INVOICE_YEAR,
        a.INVOICE_MONTH,
        a.CARE_TYPE,
        a.FUNDER_TYPE,
        a.CATEGORY_OF_CARE,
        a.TOT_AMT_INVOICED,
        a.TOT_SUM_MSF,
        a.TOT_SUM_HOURS,
        a.TOT_CNT_CUSTOMERS,
        a.TOT_CNT_CARERS,
        a.TOT_AMT_EXPENSES
    FROM all_silver a
    LEFT JOIN TEMP_HUBSPOT_COMPANIES hc
        ON UPPER(TRIM(a.COMPANY_NAME)) = UPPER(TRIM(hc.COMPANY_NAME));

    row_count := SQLROWCOUNT;

    process_date := DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE);
    curr_year := YEAR(process_date);
    curr_month := MONTH(process_date);

    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0) INTO curr_month_total
    FROM BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE
    WHERE INVOICE_YEAR = :curr_year
      AND INVOICE_MONTH = TO_CHAR(DATE_FROM_PARTS(:curr_year, :curr_month, 1), 'MON');

    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0) INTO prev_month_total
    FROM BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE
    WHERE INVOICE_YEAR = YEAR(DATEADD(MONTH, -1, DATE_FROM_PARTS(:curr_year, :curr_month, 1)))
      AND INVOICE_MONTH = TO_CHAR(DATEADD(MONTH, -1, DATE_FROM_PARTS(:curr_year, :curr_month, 1)), 'MON');

    IF (prev_month_total > 0) THEN
        variance_pct := ROUND((curr_month_total - prev_month_total) / prev_month_total * 100, 1);
    ELSE
        variance_pct := NULL;
    END IF;

    variance_msg := ' | Current: ' || TO_CHAR(COALESCE(curr_month_total, 0), '999,999,999') || 
                    ' | Previous: ' || TO_CHAR(COALESCE(prev_month_total, 0), '999,999,999') ||
                    ' | Variance: ' || COALESCE(variance_pct::STRING || '%', 'N/A');

    RETURN 'SUCCESS: Gold merge complete (' || COALESCE(row_count, 0) || ' rows)' || COALESCE(variance_msg, '');
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Runs on schedule (09:00 UTC on 1st) - after all transforms complete
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_INVOICE_ALL_MERGE
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_PASS_INVOICE_TRANSFORM, BBC_CONFORMED.ORCHESTRATE.TASK_OTH_INVOICE_TRANSFORM
    --SCHEDULE = 'USING CRON 0 9 1 * * UTC'
AS
    CALL BBC_CONFORMED.REVENUE.SP_INVOICE_ALL_MERGE();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_DWH_DEV.SEMANTICMODEL.TASK_INVOICE_SEMANTIC_GOLD RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_DWH_DEV.SEMANTICMODEL.SP_INVOICE_SEMANTIC_GOLD();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
/* 
-- Check OTH transition (Dec 2025 vs Jan 2026)
SELECT ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, SUM(TOT_AMT_INVOICED) 
FROM BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE 
--WHERE ROSTER_SYSTEM LIKE 'OTH%' AND INVOICE_YEAR IN (2025, 2026)
WHERE INVOICE_YEAR IN (2026)
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

SELECT ROSTER_SYSTEM, BRANCH_NAME_PBI, COMPANY_NAME, SUM(TOT_AMT_INVOICED) 
FROM BBC_CONFORMED.REVENUE.REVENUE_DATA_ALLMERGE 
WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 'Jan'
AND BRANCH_NAME_PBI LIKE ('%Pontypr%')
GROUP BY 1, 2, 3 
