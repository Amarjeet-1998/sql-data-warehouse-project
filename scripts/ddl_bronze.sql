-- Load csv data into bronze_cust_info database tables

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time = GETDATE();
PRINT"=========================================================";
PRINT 'Loading Bronze Layer';
PRINT"=========================================================";

PRINT'------------------------------------------------------------';
PRINT'Loading CRM Tables';
PRINT'------------------------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: Bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );
SET @end_time = GETDATE();
PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> ------------------';

--SELECT * FROM bronze.crm_cust_info

--SELECT COUNT(*) FROM bronze.crm_cust_info

--Repeat this steps for all tables
TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );

TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );

PRINT'------------------------------------------------------------';
PRINT'Loading ERP Tables';
PRINT'------------------------------------------------------------';

TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
WITH (
  FIRSTROW = 2
  FIELDTERMINATOR =',',
  TABLOCK
  );
SET @end_time = GETDATE();
PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> ------------------';

SET@batch_end_time = GETDATE();
PRINT '=====================================';
PRINT 'Loading Bronze Layer is Completed';
PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
PRINT '=====================================';
END TRY
BEGIN CATCH
  PRINT '====================================================='
  PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
  PRINT 'Error Message' + ERROR_Message();
  PRINT 'Error Message' CAST (ERROR_NUMBER() AS NVARCHAR);
  PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR;
  PRINT '====================================================='
END CATCH
END;
