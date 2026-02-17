/*
Storage integration: 
Goal: Control Azure Storage Account from SF Integration and Stage
Uses: 1. Setup Integration once, no need to pass credentials (SAS). 2. Use SF RBAC to control access to Storage containter. 3. Use SF COPY INTO for bulk loading. 4. Remove Azure storage container files after load into SF*/

use role ACCOUNTADMIN;
--SHOW INTEGRATIONS;
drop storage integration azure_int_sourceinbound;
create or replace storage integration azure_int_sourceinbound
  type = external_stage
  storage_provider = azure
  enabled = true
  azure_tenant_id = 'ac52f0d9-fff8-4327-8724-ba3c86fb4fb3'
  storage_allowed_locations = (
                'azure://adlsgen2bbcukir.blob.core.windows.net/source-inbound/',
                'azure://adlsgen2bbcukir.blob.core.windows.net/source-inbound/staffplan-bbcukir/',
                'azure://adlsgen2bbcukir.blob.core.windows.net/source-inbound/onetouch-bbcukir/' )
  --storage_blocked_locations = ('azure://myaccount.blob.core.windows.net/mycontainer/path2/')
  ;

--Snowflake will create a service principal in your Azure account that you can give reader access
--from Account Admin level, give permissions to other roles within Snowflake using Storage Integrations
GRANT USAGE ON INTEGRATION azure_int_sourceinbound TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION azure_int_sourceinbound TO ROLE ROLE_SNOWFLAKE_RW;
--SHOW ROLES;
--DESC STORAGE INTEGRATION azure_int_sourceinbound;

--create external stage
CREATE OR REPLACE STAGE BBC_SOURCE_RAW.STAFFPLAN.AZSTG_INBOUND_STAFFPLAN
    URL = 'azure://adlsgen2bbcukir.blob.core.windows.net/source-inbound/staffplan-bbcukir/'
    STORAGE_INTEGRATION = azure_int_sourceinbound
    DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE STAGE BBC_SOURCE_RAW.ONETOUCH.AZSTG_INBOUND_ONETOUCH
    URL = 'azure://adlsgen2bbcukir.blob.core.windows.net/source-inbound/onetouch-bbcukir/'
    STORAGE_INTEGRATION = azure_int_sourceinbound
    DIRECTORY = (ENABLE = TRUE);
 -- [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] ) } ]
 -- [ COPY_OPTIONS = ( copyOptions ) ]
 -- [ COMMENT = '<string_literal>' ]
