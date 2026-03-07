/*----------------------------------------------------------------------------------------
Module:   1_Invoice_Raw_OTH
Purpose:  Load OTH Invoice files from stage (4 files per month)
Schedule: Runs 1st of every month at 08:00 UTC
          IMMUTABLE: Will NOT overwrite existing tables

Files per country (134/149):
  - INVOICEDETAILS (26 cols)      - Invoice header with BRANCHNAME, VAT, SPLIT
  - INVOICEDETAILSITEMS (72 cols) - Line items with BILLING, INVOICEHRS

Tables Created:
  - OTH_<COUNTRY>_INVOICEDETAILS_YYYY_MM
  - OTH_<COUNTRY>_INVOICEDETAILSITEMS_YYYY_MM
----------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------
One-Time Setup: Table templates and file format
----------------------------------------------------------------------------------------*/
/*
-- Invoice Details template (26 columns)
CREATE OR REPLACE TABLE BBC_SOURCE_RAW.ONETOUCH.TABLESTR_OTH_INVOICEDETAILS_TEMPLATE (
    invoiceid NUMBER(38, 0),
    companyid NUMBER(38, 0),
    locationid NUMBER(38, 0),
    created TIMESTAMP_NTZ,
    updated TIMESTAMP_NTZ,
    branchid NUMBER(38, 0),
    branchname VARCHAR,
    clientid NUMBER(38, 0),
    invoicenum NUMBER(38, 0),
    split NUMBER(38, 14),
    splittype VARCHAR,
    splitvalue VARCHAR,
    invoicetype VARCHAR,
    jobtypeid VARCHAR,
    jobtypename VARCHAR,
    manualinvoice NUMBER(38, 0),
    areaid NUMBER(38, 0),
    areaname VARCHAR,
    workgroupid VARCHAR,
    workgroupname VARCHAR,
    invoicetemplateid NUMBER(38, 0),
    invoicenumber VARCHAR,
    paymentdue VARCHAR,
    periodstart DATE,
    periodfinish DATE,
    vat NUMBER(38, 0)
);

-- Invoice Details Items template (72 columns)
-- Uses VARCHAR for columns with differing types between 134 and 149
CREATE OR REPLACE TABLE BBC_SOURCE_RAW.ONETOUCH.TABLESTR_OTH_INVOICEDETAILSITEMS_TEMPLATE (
    id NUMBER(38, 0),
    type VARCHAR,
    subtype VARCHAR,
    created TIMESTAMP_NTZ,
    createdby NUMBER(38, 0),
    deleted VARCHAR,
    deletedby VARCHAR,
    invoiceid NUMBER(38, 0),
    timesheetid NUMBER(38, 0),
    expenseid VARCHAR,
    companyid NUMBER(38, 0),
    locationid NUMBER(38, 0),
    logdate DATE,
    clientid NUMBER(38, 0),
    carerid NUMBER(38, 0),
    areaid NUMBER(38, 0),
    payabletime VARCHAR,
    jobtypeid NUMBER(38, 0),
    billing NUMBER(38, 5),
    billingdecimals NUMBER(38, 5),
    billingallocatedlimited VARCHAR,
    billingcalled VARCHAR,
    billingrate VARCHAR,
    billingscaleid NUMBER(38, 0),
    billingscalename VARCHAR,
    pay NUMBER(38, 4),
    paycalled VARCHAR,
    payrate VARCHAR,
    payscaleid NUMBER(38, 0),
    payscalename VARCHAR,
    invoiceshortdate VARCHAR,
    invoiceweekdate VARCHAR,
    invoicetimeactual VARCHAR,
    invoicetimesched VARCHAR,
    invoicecarername VARCHAR,
    jobtypename VARCHAR,
    invoicejobtypename VARCHAR,
    invoicepo VARCHAR,
    invoicecarerposition VARCHAR,
    invoicehrs NUMBER(38, 6),
    invoiceallocatedlimitedhrs VARCHAR,
    discountpercent VARCHAR,
    discounttotal VARCHAR,
    invoicesplitarray VARCHAR,
    invoicesplitarraypay VARCHAR,
    invoiceadminsignid VARCHAR,
    invoiceadminsignname VARCHAR,
    invoicecarersign VARCHAR,
    invoicetravelmetric VARCHAR,
    invoicetraveldistance NUMBER(38, 2),
    invoicetraveldeduction VARCHAR,
    invoicetravelrate VARCHAR,
    invoicetravelvalue VARCHAR,
    expensedescription VARCHAR,
    wtramountvalue NUMBER(38, 2),
    wtramountpercent NUMBER(38, 2),
    comissionvalue NUMBER(38, 0),
    comissiontotal NUMBER(38, 0),
    employersnivalue VARCHAR,
    employersnipercent VARCHAR,
    preview NUMBER(38, 0),
    invoicebreakminutes VARCHAR,
    invoicebreakpaid VARCHAR,
    numberinvoicepage VARCHAR,
    billingfiscalmonth VARCHAR,
    billingweekending DATE,
    earnedfiscalmonth VARCHAR,
    earnedfiscalweek DATE,
    jobtypegroupid NUMBER(38, 0),
    jobtypegroupname VARCHAR,
    jobtypegroupcode VARCHAR,
    manualinvoice VARCHAR
);

-- File format
CREATE OR REPLACE FILE FORMAT BBC_SOURCE_RAW.ONETOUCH.FF_OTH_CSV
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    TRIM_SPACE = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = AUTO
    TIME_FORMAT = AUTO
    TIMESTAMP_FORMAT = AUTO
    NULL_IF = ('', 'NULL', 'null');
*/

/*----------------------------------------------------------------------------------------
Procedure: Load Invoice Details (26 columns)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILS_LOAD(COUNTRY STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    year_month STRING;
    table_name STRING;
    stage_file STRING;
    table_exists INT;
    row_count INT;
BEGIN
    year_month := TO_CHAR(DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE), 'YYYY_MM');
    table_name := 'OTH_' || COUNTRY || '_INVOICEDETAILS_' || year_month;
    stage_file := table_name || '.csv';
    
    SELECT COUNT(*) INTO table_exists
    FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ONETOUCH' AND TABLE_NAME = UPPER(:table_name);
    
    IF (table_exists > 0) THEN
        RETURN 'SKIPPED: ' || table_name || ' already exists';
    END IF;
    
    EXECUTE IMMEDIATE 
        'CREATE TABLE BBC_SOURCE_RAW.ONETOUCH.' || table_name || 
        ' LIKE BBC_SOURCE_RAW.ONETOUCH.TABLESTR_OTH_INVOICEDETAILS_TEMPLATE';
    
    EXECUTE IMMEDIATE
        'COPY INTO BBC_SOURCE_RAW.ONETOUCH.' || table_name || '
         FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26
               FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_INVOICING/' || stage_file || ')
         FILE_FORMAT = BBC_SOURCE_RAW.ONETOUCH.FF_OTH_CSV
         ON_ERROR = ABORT_STATEMENT';
    
    row_count := SQLROWCOUNT;
    
    RETURN 'SUCCESS: ' || table_name || ' (' || row_count || ' rows)';
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Load Invoice Details Items (72 columns)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILSITEMS_LOAD(COUNTRY STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    year_month STRING;
    table_name STRING;
    stage_file STRING;
    table_exists INT;
    row_count INT;
BEGIN
    year_month := TO_CHAR(DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE), 'YYYY_MM');
    table_name := 'OTH_' || COUNTRY || '_INVOICEDETAILSITEMS_' || year_month;
    stage_file := table_name || '.csv';
    
    SELECT COUNT(*) INTO table_exists
    FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ONETOUCH' AND TABLE_NAME = UPPER(:table_name);
    
    IF (table_exists > 0) THEN
        RETURN 'SKIPPED: ' || table_name || ' already exists';
    END IF;
    
    EXECUTE IMMEDIATE 
        'CREATE TABLE BBC_SOURCE_RAW.ONETOUCH.' || table_name || 
        ' LIKE BBC_SOURCE_RAW.ONETOUCH.TABLESTR_OTH_INVOICEDETAILSITEMS_TEMPLATE';
    
    EXECUTE IMMEDIATE
        'COPY INTO BBC_SOURCE_RAW.ONETOUCH.' || table_name || '
         FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                      $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
                      $41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,
                      $61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72
               FROM @EXTERNAL_INTEGRATIONS.OTH_TO_BBC.STG_OTH_TO_BBC_INVOICING/' || stage_file || ')
         FILE_FORMAT = BBC_SOURCE_RAW.ONETOUCH.FF_OTH_CSV
         ON_ERROR = ABORT_STATEMENT';
    
    row_count := SQLROWCOUNT;
    
    RETURN 'SUCCESS: ' || table_name || ' (' || row_count || ' rows)';
END;
$$;

/*----------------------------------------------------------------------------------------
Procedure: Orchestrator - loads all 4 files (both types × both countries)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICE_LOAD_ALL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    r1 STRING; r2 STRING; r3 STRING; r4 STRING;
BEGIN
    CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILS_LOAD('134') INTO r1;
    CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILSITEMS_LOAD('134') INTO r2;
    CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILS_LOAD('149') INTO r3;
    CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILSITEMS_LOAD('149') INTO r4;
    
    RETURN r1 || ' | ' || r2 || ' | ' || r3 || ' | ' || r4;
END;
$$;

/*----------------------------------------------------------------------------------------
Task: Monthly invoice data load (1st of month at 08:00 UTC)
----------------------------------------------------------------------------------------*/
CREATE OR REPLACE TASK BBC_CONFORMED.ORCHESTRATE.TASK_OTH_INVOICE_MONTHLY_LOAD
    WAREHOUSE = REPORT_WH
    AFTER BBC_CONFORMED.ORCHESTRATE.TASK_INVOICE_ROOT
AS
    CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICE_LOAD_ALL();

/*----------------------------------------------------------------------------------------
Setup
----------------------------------------------------------------------------------------*/
-- First run the One-Time Setup section above to create templates and file format
-- ALTER TASK BBC_SOURCE_RAW.ONETOUCH.TASK_OTH_INVOICE_MONTHLY_LOAD RESUME;

/*----------------------------------------------------------------------------------------
Manual Execution
----------------------------------------------------------------------------------------*/
-- CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILS_LOAD('134');
-- CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILSITEMS_LOAD('134');
-- CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILS_LOAD('149');
-- CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICEDETAILSITEMS_LOAD('149');
-- CALL BBC_SOURCE_RAW.ONETOUCH.SP_OTH_INVOICE_LOAD_ALL();

/*----------------------------------------------------------------------------------------
Validation
----------------------------------------------------------------------------------------*/
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.ONETOUCH.OTH_134_INVOICEDETAILS_2026_01;
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.ONETOUCH.OTH_134_INVOICEDETAILSITEMS_2026_01;
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.ONETOUCH.OTH_149_INVOICEDETAILS_2026_01;
-- SELECT COUNT(*) FROM BBC_SOURCE_RAW.ONETOUCH.OTH_149_INVOICEDETAILSITEMS_2026_01;

/*----------------------------------------------------------------------------------------
Maintenance - KEEP
----------------------------------------------------------------------------------------*/
-- Preview what will be dropped
/*
SELECT TABLE_NAME FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'ONETOUCH' AND TABLE_NAME LIKE 'OTH_134_INVOICEDETAILS%';

-- Drop all matching tables
DECLARE
    cur CURSOR FOR 
        SELECT TABLE_NAME FROM BBC_SOURCE_RAW.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'ONETOUCH' AND TABLE_NAME LIKE 'OTH_134_INVOICEDETAILS%';
BEGIN
    FOR rec IN cur DO
        EXECUTE IMMEDIATE 'DROP TABLE BBC_SOURCE_RAW.ONETOUCH.' || rec.TABLE_NAME;
    END FOR;
END;
*/

/*
SUCCESS: OTH_134_INVOICEDETAILS_2026_02 (10701 rows) | SUCCESS: OTH_134_INVOICEDETAILSITEMS_2026_02 (241649 rows) | SUCCESS: OTH_149_INVOICEDETAILS_2026_02 (4409 rows) | SUCCESS: OTH_149_INVOICEDETAILSITEMS_2026_02 (153258 rows)
*/