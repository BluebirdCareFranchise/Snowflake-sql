/*----------------------------------------------------------------------------------------
Module:   2_Invoice_Refine_PASS
Purpose:  Transform PASS raw invoice data into DETAIL and SUMM tables
Source:   BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_
Output:   
  DETAIL table (invoice-level, pre-aggregation):
  - BBC_CONFORMED.PASS.PASS_INVOICE_DETAIL
  
  SUMM tables (aggregated):
  - BBC_CONFORMED.PASS.PASS_INVOICE_SUMM
  - BBC_CONFORMED.PASS.PASS_HOURS_SUMM

Flow:     Raw → DETAIL (with INV_NO, OFFICE_ID, etc.) → SUMM (aggregated)

Usage:
  CALL BBC_DWH_DEV.SEMANTICMODEL.SP_PASS_INVOICE_TRANSFORM();
----------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE BBC_REFINED.PASS.SP_PASS_INVOICE_TRANSFORM()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    detail_count INT DEFAULT 0;
    inv_count INT DEFAULT 0;
    hrs_count INT DEFAULT 0;
    curr_month_total NUMBER DEFAULT 0;
    prev_month_total NUMBER DEFAULT 0;
    variance_pct NUMBER;
    variance_msg STRING DEFAULT '';
    curr_year INT;
    curr_month INT;
    process_date DATE;
BEGIN
    USE DATABASE BBC_DWH_DEV;
    USE SCHEMA SEMANTICMODEL;

    /*====================================================================================
    Step 1: Create DETAIL table with invoice movement (LAG logic)
    ====================================================================================*/
    CREATE TABLE IF NOT EXISTS BBC_CONFORMED.PASS.PASS_INVOICE_DETAIL (
        ROSTER_SYSTEM STRING,
        OFFICE_ID NUMBER,
        OFFICE_NAME STRING,
        TERRITORY STRING,
        INVOICE_YEAR NUMBER,
        INVOICE_MONTH NUMBER,
        CARE_TYPE STRING,
        FUNDER_TYPE STRING,
        INV_NO STRING,
        TOT_AMT_INVOICED NUMBER
    );

    TRUNCATE TABLE BBC_CONFORMED.PASS.PASS_INVOICE_DETAIL;

    INSERT INTO BBC_CONFORMED.PASS.PASS_INVOICE_DETAIL
    WITH invoice_base AS (
        SELECT
            OFFICE_ID,
            CASE 
                WHEN OFFICE_NAME LIKE '%Longford, Roscommon and Westmeath%' THEN 'PASS-IRL'
                ELSE 'PASS-UK'
            END AS ROSTER_SYSTEM,
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE(OFFICE_NAME, 'Bluebird [Cc]are |BBC ', ''),
                '[()-]', ''
            )) AS OFFICE_NAME,
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE(TERRITORY, 'Bluebird [Cc]are |BBC ', ''),
                '[()-]', ''
            )) AS TERRITORY,
            INV_NO,
            CARE_TYPE,
            FUNDER_TYPE,
            INVOICE_DATE,
            COALESCE(INVOICE_SENT_DATE, INVOICE_DATE) AS INVOICE_SENT_DATE,
            TOTAL_INVOICED
        FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_
        WHERE TOTAL_INVOICED IS NOT NULL
    ),
    latest_sent AS (
        SELECT
            *,
            DATE_TRUNC('MONTH', INVOICE_SENT_DATE) AS SENT_MONTH
        FROM invoice_base
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                OFFICE_ID,
                INV_NO,
                CARE_TYPE,
                FUNDER_TYPE,
                DATE_TRUNC('MONTH', INVOICE_SENT_DATE)
            ORDER BY INVOICE_SENT_DATE DESC
        ) = 1
    ),
    invoice_movement AS (
        SELECT
            ROSTER_SYSTEM,
            OFFICE_ID,
            OFFICE_NAME,
            TERRITORY,
            CARE_TYPE,
            FUNDER_TYPE,
            YEAR(SENT_MONTH)  AS INVOICE_YEAR,
            MONTH(SENT_MONTH) AS INVOICE_MONTH,
            INV_NO,
            TOTAL_INVOICED - COALESCE(
                LAG(TOTAL_INVOICED) OVER (
                    PARTITION BY
                        OFFICE_ID,
                        INV_NO,
                        CARE_TYPE,
                        FUNDER_TYPE
                    ORDER BY SENT_MONTH
                ),
                0
            ) AS TOT_AMT_INVOICED
        FROM latest_sent
    )
    SELECT
        ROSTER_SYSTEM,
        OFFICE_ID,
        OFFICE_NAME,
        TERRITORY,
        INVOICE_YEAR,
        INVOICE_MONTH,
        CARE_TYPE,
        FUNDER_TYPE,
        INV_NO,
        TOT_AMT_INVOICED
    FROM invoice_movement;

    detail_count := SQLROWCOUNT;

    /*====================================================================================
    Step 2: Create final PASS_INVOICE_SUMM with aggregation from DETAIL
    ====================================================================================*/
    CREATE OR REPLACE TABLE BBC_CONFORMED.PASS.PASS_INVOICE_SUMM AS
    SELECT
        CASE
            WHEN TERRITORY IN ('', 'Untagged') OR TERRITORY IS NULL
            THEN OFFICE_NAME
            ELSE TERRITORY
        END AS COMPANY_NAME,
        ROSTER_SYSTEM,
        INVOICE_YEAR,
        TO_CHAR(DATE_FROM_PARTS(INVOICE_YEAR, INVOICE_MONTH, 1), 'MON') AS INVOICE_MONTH,
        CARE_TYPE,
        FUNDER_TYPE,
        ROUND(SUM(TOT_AMT_INVOICED), 0) AS TOT_AMT_INVOICED,
        NULL AS TOT_SUM_MSF,
        NULL AS TOT_SUM_HOURS,
        NULL AS TOT_CNT_CUSTOMERS,
        NULL AS TOT_CNT_CARERS
    FROM BBC_CONFORMED.PASS.PASS_INVOICE_DETAIL
    GROUP BY 1, 2, 3, 4, 5, 6;

    inv_count := SQLROWCOUNT;

    process_date := DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE);
    curr_year := YEAR(process_date);
    curr_month := MONTH(process_date);

    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0) INTO curr_month_total
    FROM BBC_CONFORMED.PASS.PASS_INVOICE_SUMM
    WHERE INVOICE_YEAR = :curr_year
      AND INVOICE_MONTH = TO_CHAR(DATE_FROM_PARTS(:curr_year, :curr_month, 1), 'MON');

    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0) INTO prev_month_total
    FROM BBC_CONFORMED.PASS.PASS_INVOICE_SUMM
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

    /*====================================================================================
    Step 3: Create PASS_HOURS_SUMM
    ====================================================================================*/
    CREATE OR REPLACE TABLE BBC_CONFORMED.PASS.PASS_HOURS_SUMM AS
    SELECT
        OFFICE_ID,
        YEAR(PERIOD_END)  AS INVOICE_YEAR,
        MONTH(PERIOD_END) AS INVOICE_MONTH,
        ROUND(AVG(CUSTOMERS), 0)     AS TOT_CNT_CUSTOMERS,
        ROUND(SUM(PERIOD_VISITS), 0) AS TOT_CNT_VISITS,
        ROUND(SUM(CHARGED_HOURS), 0) AS TOT_CNT_HOURS
    FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_PERIOD_VISIT_
    GROUP BY 1, 2, 3;

    hrs_count := SQLROWCOUNT;

    RETURN 'SUCCESS: PASS Detail (' || COALESCE(detail_count, 0) || ' rows) | Invoice (' || COALESCE(inv_count, 0) || ' rows) | Hours (' || COALESCE(hrs_count, 0) || ' rows)' || COALESCE(variance_msg, '');
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Runs on schedule (monthly)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_PASS_INVOICE_TRANSFORM
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_PASS_INVOICE_MONTHLY_LOAD
AS
    CALL BBC_REFINED.PASS.SP_PASS_INVOICE_TRANSFORM();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_DWH_DEV.SEMANTICMODEL.TASK_PASS_INVOICE_TRANSFORM RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_DWH_DEV.SEMANTICMODEL.SP_PASS_INVOICE_TRANSFORM();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
/*
-- DETAIL table (invoice-level)
SELECT * FROM BBC_REFINED.PASS.PASS_INVOICE_DETAIL 
WHERE INVOICE_YEAR = 2026 LIMIT 100;

-- SUMM table (aggregated)
SELECT ROSTER_SYSTEM, INVOICE_YEAR, INVOICE_MONTH, COUNT(*), SUM(TOT_AMT_INVOICED) 
FROM BBC_REFINED.PASS.PASS_INVOICE_SUMM 
GROUP BY 1, 2, 3 ORDER BY 1, 2;

SELECT  
    MONTH(COALESCE(INVOICE_SENT_DATE, INVOICE_DATE)) AS INVOICE_SENT_DATE, OFFICE_NAME,
    TERRITORY, SUM(TOTAL_INVOICED) 
FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_
where YEAR(COALESCE(INVOICE_SENT_DATE, INVOICE_DATE)) = 2026
AND INVOICE_SENT_DATE is null
GROUP BY 1, 2,3
ORDER BY OFFICE_NAME, TERRITORY;

SELECT  *
FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_
where INVOICE_SENT_DATE is null
and YEAR(COALESCE(INVOICE_SENT_DATE, INVOICE_DATE)) = 2026

SELECT  *
FROM BBC_SOURCE_RAW.PASS.VW_PASS_SHARED_BBC_SUMMARY_INVOICE_
where INVOICE_SENT_DATE is null
and YEAR(COALESCE(INVOICE_SENT_DATE, INVOICE_DATE)) = 2026
*/
11549461
11260920