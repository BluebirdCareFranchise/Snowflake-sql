/*----------------------------------------------------------------------------------------
Module:   2_Invoice_Refine_OTH
Purpose:  Transform OTH raw invoice data into DETAIL and SUMM tables (per country)
Depends:  OTH_INV_1_CopyStage_ViewDef.sql
Output:   
  DETAIL tables (line-level, pre-aggregation):
  - BBC_REFINED.OTH.OTH_134_INVOICE_DETAIL (UK)
  - BBC_REFINED.OTH.OTH_149_INVOICE_DETAIL (IRL)
  
  SUMM tables (aggregated):
  - BBC_REFINED.OTH.OTH_134_INVOICE_SUMM (UK)
  - BBC_REFINED.OTH.OTH_149_INVOICE_SUMM (IRL)

Joins:    INVOICEDETAILS (header: BRANCHNAME, VAT, SPLIT, MANUALINVOICE)
        + INVOICEDETAILSITEMS (items: BILLING, INVOICEHRS, JOBTYPEGROUPNAME)

Flow:     Raw → DETAIL (with INVOICEID, MANUALINVOICE, etc.) → SUMM (aggregated)

Usage:
  CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('134', '2026-01-01');  -- UK, Jan 2026
  CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('149', '2026-01-01');  -- IRL, Jan 2026
  CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM_ALL('2026-01-01');     -- Both
----------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM(COUNTRY STRING, TARGET_MONTH DATE)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    process_month DATE;
    year_month STRING;
    roster_system STRING;
    target_table STRING;
    details_table STRING;
    items_table STRING;
    row_count INT;
    curr_month_total NUMBER DEFAULT 0;
    prev_month_total NUMBER DEFAULT 0;
    variance_pct NUMBER;
    variance_msg STRING;
BEGIN
    process_month := COALESCE(:TARGET_MONTH, DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE)));
    year_month := TO_CHAR(process_month, 'YYYY_MM');
    roster_system := CASE :COUNTRY 
                         WHEN '134' THEN 'OTH-UK' 
                         WHEN '149' THEN 'OTH-IRL' 
                         ELSE 'OTH-UNKNOWN' 
                     END;
    target_table := 'BBC_REFINED.OTH.OTH_' || :COUNTRY || '_INVOICE_SUMM';
    details_table := 'BBC_SOURCE_RAW.ONETOUCH.OTH_' || :COUNTRY || '_INVOICEDETAILS_' || year_month;
    items_table := 'BBC_SOURCE_RAW.ONETOUCH.OTH_' || :COUNTRY || '_INVOICEDETAILSITEMS_' || year_month;

    EXECUTE IMMEDIATE '
    CREATE TABLE IF NOT EXISTS ' || target_table || ' (
        ROSTER_SYSTEM STRING, BRANCHID NUMBER, BRANCHNAME STRING,
        INVOICE_YEAR NUMBER, INVOICE_MONTH NUMBER, TERRITORY STRING,
        CATEGORY_OF_CARE STRING, FUNDER_TYPE STRING, CARE_TYPE STRING,
        TOT_AMT_INVOICED NUMBER, TOT_AMT_EXPENSES NUMBER, TOT_CNT_HOURS NUMBER
    )';

    EXECUTE IMMEDIATE '
    CREATE TABLE IF NOT EXISTS ' || REPLACE(target_table, '_SUMM', '_DETAIL') || ' (
        ROSTER_SYSTEM STRING, BRANCHID NUMBER, BRANCHNAME STRING,
        INVOICE_YEAR NUMBER, INVOICE_MONTH NUMBER,
        CATEGORY_OF_CARE STRING, FUNDER_TYPE STRING,
        INVOICEID NUMBER, MANUALINVOICE NUMBER, WORKGROUPID STRING,
        DISCOUNTPERCENT STRING, VAT NUMBER, SPLIT NUMBER, TYPE STRING,
        TOT_AMT_BILLING NUMBER, TOT_CNT_HOURS NUMBER
    )';

    EXECUTE IMMEDIATE '
    DELETE FROM ' || REPLACE(target_table, '_SUMM', '_DETAIL') || '
    WHERE INVOICE_YEAR = YEAR(''' || process_month || '''::DATE)
      AND INVOICE_MONTH = MONTH(''' || process_month || '''::DATE)';

    EXECUTE IMMEDIATE '
    INSERT INTO ' || REPLACE(target_table, '_SUMM', '_DETAIL') || '
        (ROSTER_SYSTEM, BRANCHID, BRANCHNAME, INVOICE_YEAR, INVOICE_MONTH,
         CATEGORY_OF_CARE, FUNDER_TYPE, INVOICEID, MANUALINVOICE, WORKGROUPID,
         DISCOUNTPERCENT, VAT, SPLIT, TYPE, TOT_AMT_BILLING, TOT_CNT_HOURS)
    SELECT
        ''' || roster_system || ''' AS ROSTER_SYSTEM,
        inv.BRANCHID,
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(
                REPLACE(REPLACE(inv.BRANCHNAME, ''&amp;'', ''&''), '' OneTouch'', ''''),
                ''Bluebird [Cc]are |BBC '', ''''
            ),
            ''[()-]'', ''''
        )) AS BRANCHNAME,
        YEAR(COALESCE(inv.UPDATED, inv.CREATED)) AS INVOICE_YEAR,
        MONTH(COALESCE(inv.UPDATED, inv.CREATED)) AS INVOICE_MONTH,
        itm.JOBTYPEGROUPNAME AS CATEGORY_OF_CARE,
        inv.JOBTYPENAME AS FUNDER_TYPE,
        inv.INVOICEID,
        inv.MANUALINVOICE,
        inv.WORKGROUPID,
        itm.DISCOUNTPERCENT,
        inv.VAT,
        inv.SPLIT,
        itm.TYPE,
        ROUND(SUM(itm.BILLING), 2) AS TOT_AMT_BILLING,
        ROUND(SUM(itm.INVOICEHRS), 2) AS TOT_CNT_HOURS
    FROM (
        SELECT * FROM ' || details_table || '
        QUALIFY ROW_NUMBER() OVER (PARTITION BY INVOICEID ORDER BY COALESCE(UPDATED, CREATED) DESC) = 1
    ) inv
    INNER JOIN ' || items_table || ' itm ON inv.INVOICEID = itm.INVOICEID
    WHERE itm.TYPE IN (''timesheet'', ''expense'', ''cancelled'')
      AND itm.DELETED IS NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14';

    EXECUTE IMMEDIATE '
    MERGE INTO ' || target_table || ' tgt
    USING (
        WITH invoice_calc AS (
            SELECT
                ROSTER_SYSTEM,
                BRANCHID,
                BRANCHNAME,
                INVOICE_YEAR,
                INVOICE_MONTH,
                CATEGORY_OF_CARE,
                FUNDER_TYPE,
                SUM(CASE WHEN TYPE <> ''expense'' 
                    THEN IFF(SPLIT > 0, TOT_AMT_BILLING * SPLIT / 100, TOT_AMT_BILLING)
                         * (1 - COALESCE(IFF(TRY_TO_NUMBER(DISCOUNTPERCENT) > 100 OR TRY_TO_NUMBER(DISCOUNTPERCENT) < 0, 0, TRY_TO_NUMBER(DISCOUNTPERCENT)), 0) / 100)
                    ELSE 0 END) AS TOTAL_BILLING,
                SUM(CASE WHEN TYPE = ''expense''
                    THEN IFF(SPLIT > 0, TOT_AMT_BILLING * SPLIT / 100, TOT_AMT_BILLING)
                    ELSE 0 END) AS TOTAL_EXPENSES,
                SUM(TOT_CNT_HOURS) AS TOT_CNT_HOURS,
                MAX(VAT) AS VAT
            FROM ' || REPLACE(target_table, '_SUMM', '_DETAIL') || '
            WHERE (MANUALINVOICE <> 2 OR MANUALINVOICE IS NULL)
              AND INVOICE_YEAR = YEAR(''' || process_month || '''::DATE)
              AND INVOICE_MONTH = MONTH(''' || process_month || '''::DATE)
              AND BRANCHID IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        )
        SELECT
            ROSTER_SYSTEM,
            BRANCHID,
            BRANCHNAME,
            INVOICE_YEAR,
            INVOICE_MONTH,
            '''' AS TERRITORY,
            CATEGORY_OF_CARE,
            FUNDER_TYPE,
            '''' AS CARE_TYPE,
            ROUND(SUM(
                TOTAL_BILLING
                * IFF(VAT IS NOT NULL AND VAT <> 99, 1 + VAT / 100, 1)
            ), 2) AS TOT_AMT_INVOICED,                                                                                                                       
            ROUND(SUM(
                TOTAL_EXPENSES * IFF(VAT IS NOT NULL AND VAT <> 99, 1 + VAT / 100, 1)
            ), 2) AS TOT_AMT_EXPENSES,
            ROUND(SUM(TOT_CNT_HOURS), 2) AS TOT_CNT_HOURS
        FROM invoice_calc
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ) src
    ON  tgt.BRANCHID = src.BRANCHID
    AND tgt.INVOICE_YEAR = src.INVOICE_YEAR
    AND tgt.INVOICE_MONTH = src.INVOICE_MONTH
    AND COALESCE(tgt.CATEGORY_OF_CARE, '''') = COALESCE(src.CATEGORY_OF_CARE, '''')
    AND COALESCE(tgt.FUNDER_TYPE, '''') = COALESCE(src.FUNDER_TYPE, '''')
    WHEN MATCHED THEN UPDATE SET
        tgt.BRANCHNAME = src.BRANCHNAME,
        tgt.TOT_AMT_INVOICED = src.TOT_AMT_INVOICED,
        tgt.TOT_AMT_EXPENSES = src.TOT_AMT_EXPENSES,
        tgt.TOT_CNT_HOURS = src.TOT_CNT_HOURS
    WHEN NOT MATCHED THEN INSERT (
        ROSTER_SYSTEM, BRANCHID, BRANCHNAME, INVOICE_YEAR, INVOICE_MONTH,
        TERRITORY, CATEGORY_OF_CARE, FUNDER_TYPE, CARE_TYPE,
        TOT_AMT_INVOICED, TOT_AMT_EXPENSES, TOT_CNT_HOURS
    ) VALUES (
        src.ROSTER_SYSTEM, src.BRANCHID, src.BRANCHNAME, src.INVOICE_YEAR, src.INVOICE_MONTH,
        src.TERRITORY, src.CATEGORY_OF_CARE, src.FUNDER_TYPE, src.CARE_TYPE,
        src.TOT_AMT_INVOICED, src.TOT_AMT_EXPENSES, src.TOT_CNT_HOURS
    )';

    row_count := SQLROWCOUNT;

    EXECUTE IMMEDIATE '
    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0)
    FROM ' || target_table || '
    WHERE INVOICE_YEAR = YEAR(''' || process_month || '''::DATE)
      AND INVOICE_MONTH = MONTH(''' || process_month || '''::DATE)';
    SELECT $1 INTO curr_month_total FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    EXECUTE IMMEDIATE '
    SELECT COALESCE(SUM(TOT_AMT_INVOICED), 0)
    FROM ' || target_table || '
    WHERE INVOICE_YEAR = YEAR(DATEADD(MONTH, -1, ''' || process_month || '''::DATE))
      AND INVOICE_MONTH = MONTH(DATEADD(MONTH, -1, ''' || process_month || '''::DATE))';
    SELECT $1 INTO prev_month_total FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (prev_month_total > 0) THEN
        variance_pct := ROUND((curr_month_total - prev_month_total) / prev_month_total * 100, 1);
    ELSE
        variance_pct := NULL;
    END IF;

    variance_msg := ' | Current: ' || TO_CHAR(curr_month_total, '999,999,999') || 
                    ' | Previous: ' || TO_CHAR(prev_month_total, '999,999,999') ||
                    ' | Variance: ' || COALESCE(variance_pct::STRING || '%', 'N/A');

    RETURN 'SUCCESS: ' || roster_system || ' ' || year_month || ' (' || row_count || ' rows merged)' || variance_msg;
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Orchestrator - processes both countries
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM_ALL(TARGET_MONTH DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    r1 STRING;
    r2 STRING;
BEGIN
    CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('134', :TARGET_MONTH) INTO r1;
    CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('149', :TARGET_MONTH) INTO r2;
    RETURN r1 || ' | ' || r2;
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Monthly transform (runs AFTER OTH stage copy)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_OTH_INVOICE_TRANSFORM
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_OTH_INVOICE_MONTHLY_LOAD
AS
    CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM_ALL(NULL);

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- ALTER TASK BBC_SOURCE_RAW.ONETOUCH.TASK_OTH_INVOICE_TRANSFORM RESUME;
-- ALTER TASK BBC_SOURCE_RAW.ONETOUCH.TASK_OTH_INVOICE_MONTHLY_LOAD RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('134', NULL);
-- CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM('149', NULL);
-- CALL BBC_REFINED.OTH.SP_OTH_INVOICE_TRANSFORM_ALL(NULL);

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SUMM tables (aggregated)
-- SELECT COUNT(*), SUM(TOT_AMT_INVOICED) FROM BBC_REFINED.OTH.OTH_134_INVOICE_SUMM WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 2;
-- SELECT COUNT(*), SUM(TOT_AMT_INVOICED) FROM BBC_REFINED.OTH.OTH_149_INVOICE_SUMM  WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 1;
/*
-- DETAIL tables (line-level with MANUALINVOICE, INVOICEID, etc.)
SELECT * FROM BBC_REFINED.OTH.OTH_134_INVOICE_DETAIL 
WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 1 -- AND Discountpercent is not null
AND BRANCHNAME LIKE ('%Pontypr%');
SELECT SUM(TOT_AMT_BILLING) FROM BBC_REFINED.OTH.OTH_134_INVOICE_DETAIL 
WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 1 -- AND Discountpercent is not null
AND BRANCHNAME LIKE ('%Pontypr%');

SELECT SUM(TOT_AMT_INVOICED) FROM BBC_REFINED.OTH.OTH_134_INVOICE_SUMM
WHERE INVOICE_YEAR = 2026 AND INVOICE_MONTH = 1
AND BRANCHNAME LIKE ('%Pontypr%');

-- SELECT * FROM BBC_REFINED.OTH.OTH_149_INVOICE_DETAIL WHERE INVOICE_YEAR = 2026 AND BRANCHID = 832
--AND BRANCHNAME LIKE ('%Dublin South%') LIMIT 100;
*/