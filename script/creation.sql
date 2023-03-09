-- Execute this script to get the data from the Azure Blob Storage into the SQL Serverless instance
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://<storageaccount>.dfs.core.windows.net/<container>/<folder>/<filename>.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

-- Execute the following statements in SQL Dedicated Pool

-- Credentials for the Azure Blob Storage as a Managed Identity
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
    IDENTITY = 'MANAGED IDENTITY'
;

-- External Data Source for the Azure Blob Storage
CREATE EXTERNAL DATA SOURCE BlobStorage
WITH
(
    TYPE = Hadoop,
    LOCATION = 'abfss://<container>@<storageaccount>.dfs.core.windows.net/',
    CREDENTIAL = [AzureStorageCredential]
);


-- External File Format for the Azure Blob Storage
CREATE EXTERNAL FILE FORMAT parquet_file
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- Create the table in the SQL Dedicated Pool instance
CREATE EXTERNAL TABLE [dbo].[FactCallCenter]
(
    FactCallCenterID int
    ,DateKey int
    ,WageType varchar(100)
    ,Shift varchar(100)
    ,LevelOneOperators int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,LevelTwoOperators int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,TotalOperators int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,Calls int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,AutomaticResponses int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,Orders int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,IssuesRaised int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,AverageTimePerIssue int MASKED WITH (FUNCTION = 'random(1, 100)')
    ,ServiceGrade FLOAT MASKED WITH (FUNCTION = 'random(1, 100)')
    ,Date DATETIME MASKED WITH (FUNCTION = 'default()')
)
WITH
(
    LOCATION = '/<folder>/<filename>.parquet',
    DATA_SOURCE = BlobStorage,
    FILE_FORMAT = parquet_file,
    REJECT_TYPE = value,
    REJECT_VALUE = 0
);

-- Validate the data
select * from [dbo].[FactCallCenter]

-- Create a user and grant permissions to the table as select
CREATE USER test WITHOUT LOGIN;  
GRANT SELECT ON dbo.factcallcenter to test

-- Execute together both statements 
EXECUTE as user = 'test';
select * from [dbo].[FactCallCenter]