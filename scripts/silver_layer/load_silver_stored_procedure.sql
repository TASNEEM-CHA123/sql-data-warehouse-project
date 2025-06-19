/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


/*  ### General way to insert ----
INSERT INTO silver_TABLE (
			cst_id, 
			cst_key, 
			ALL THE COLUMN NAME 
		)

 SELECT 
  COL AND TRANFORMATIONS
 
 FROM bronze _table 
=================================================================================
*/



/*
=================================================================================================
#### TRANSFORMATIONS -- all applied in Select field 

-- SELECT 
   col ke saath transformation

-- Checks --
    1. Initailly checks on Bronze layer -- 1st se column start till end column -- then transformation on each column b4 moving to next col-- then check on transformend column
	2. Then drop and Insert into -- Silver table -- all the transformed column 
	3. Lastly -- check on silver table -- to see if data is correct or not . -- column wise checks 
--------------------------------------------------------------------------------------------
## 1.Check for NULL and DUPLICATES in Primary Key --

SELECT 
id , 
COUNT(*)
FROM table 
GROUP BY id
HAVING Count(*)>1 OR id IS NULL

--- NOW CHECK FOR ONE ID --
SELECT 
*
FROM table
WHERE id = 10

## 1. Remove NULL and DUPLICATE -

-- USE subquery and Row_Number -- ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS flag_last 
-- ROW_NUMBER() OVER (partion by order by or just order by etc ) as row 
-- row is alias for row _ number -- row pe condition outside subquey  
-- outside subquery t -- to show alias of subquery 
-- outise subquery row pe condition  -- 
-- like -- WHERE salary_row_number <= 5 , WHERE first_p = 1

	FROM (
			SELECT                                                                                 
				*,                                                                                 
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS flag_last 
			FROM table                                                              
			WHERE id IS NOT NULL                                                               
		) t 
    WHERE flag_last = 1;
--------------------------------------------------------------------------------------------------------

## 2. To check -- unwanted spaces in strings - like - name 

SELECT 
col
FROM table 
WHERE col!= TRIM(col)

## 2. Remove - UNWANTED SPACES 
SELECT
    id,
	key,
	TRIM(cst_firstname) AS cst_firstname,    
	TRIM(cst_lastname) AS cst_lastname,
FROM table 
---------------------------------------------------------------------------------------------------------------------

## 3. To Check data is Normalized and standardized or not --

SELECT DISTINCT col- for which u want to check 
FROM table 


## 3. To Normalize and standardize --

SELECT
	id,
	key,
	    CASE                                     -- case -- Data Standardization and consistency -- standard --only single and married , upper and trim used  
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status, -- Normalize marital status values to readable format

		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr, -- Normalize gender values to readable format

FROM table
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 4. Dervied Column 
 -- when one column of one table - have value of 2 column and we derived it
 -- chwcked by checking other tables -- [prd_key] in prd_ table had category key and product key
 -- prd_key = AC-RF-FR-R928-58 which is =  (cat_id = AC_RF) + (prd_key = FR-R928-58 or FR-R929 )  

   1. cat_id ---- REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,   -- replace(string = substring , thing to replace = '-' , with whom it would be replaced = '_')	
   2. prd_key --- SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key

## 4. NOW check if correct -- derved column--

      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  -- of prd_table
	  NOT IN 
	  SELECT sls_prd_key 
	  FROM bronze.crm_sales_detail                  -- checking in sales_table
 
 IF Output = nothing -- good all the key exist in other 
 IF Output - meaning these keys doesnt exist in another table 
           - ok only when prd_ key is not in sales as prd may not been sold - therfore not in gives output - in all other cases not in should not gove output 

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 5. Handling missing values --
     
	 ISNULL(prd_cost, 0) AS prd_cost,                       -- if prd_cost 0 make it null , replace 0 with null 

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
## 6. CHECK IF - Dates if not correct based on Rules ---

RULES -- 1. START DATE < END DATE 
         2. NO OVERLAPPING IN SALES - FOR SAME PRODUCT -- IF A PRODUCT WITH SAME ID FOR A ONE COUNTRY -- ITS START DATE OF ONE RECORD SHOULD END WHEN IT IS ORDERED FROM ANOTHER COUNTY 
                                    - FOR same id , end date of row 1 < start date of row 2 
		 3. In case - date = 2000-12-26 00:00:00 - HH:MM:SS is 00 eevrytime - change data type - cast 


## 6. DATA ENRICHMENT --
       -- AS end date doesnt follow rules -- create from scratch ==
	   -- end date of current row = start date of next row - 1 day - so no overlapping b/w dates of current and next row 


       1.convert into date datatype for START_DATE ---  CAST(prd_start_dt AS DATE) AS prd_start_dt, 

	   2. end date of row 1 = startdat of next row - 1 day -- using window function lead -- OVER window = (partition by id and orderby date )

	        CAST
			(  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1  -- lead and lag window function lead(col, offset = 1 by default)
				AS DATE                         
			) AS prd_end_dt -- Calculate end date as one day before the next start date

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

## 7. CHECK if -- Same (ALL OF THEM) key exist in other table column which we will join with this table --
	  
	  SELECT * , --- write all column name 
	  NOT IN 
	  FROM bronze.crm_sales_detail -- current table
	  WHERE key1 NOT IN (SELECT key2 FROM silver_prd_table ) -- here this is a already transformed table in silver -- other table and key name may be different but values same 
                                                             -- key1 and key2 values are same name may be same or differnt 
															 -- key 1 exist in current table , key 2 exist in other table on which we will connect

    OUTPUT -- output should be none/nothing --- means all key exist 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 8. CHECKS on DATE -- according to rules --
       
	              (** if dates are given as integer -- 20101229  --- 2010 year , 12 month , 29 date) 
	
	RULES -- 1. order date -- -ve <=0 --(if -ve or 0) THEN NULL
	         2. order date length != 8                THEN NULL
			 3. order date in boundary -- order date < 20500101 , order date >19800101 -- start date of restaurant 
		   **4. order date < shipping and due date AND Convert Data Type
    
	SELECT 
	  NULL IF (sls_order_date,0)
	  FROM bronze.crm_sales_detail 
	  WHERE sls_order_date <=0
	     OR LEN(sls_order_date) !=8
		 OR sls_order_date < 20500101
		 OR sls_order_date >19800101

## 8. TRNASFORMATION --

            CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL          -- make null if -ve or 0
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)                   -- convert data type     
			END AS sls_order_dt,

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 9. CHECKS on sales,quantity and price -- according to Business rules --
	
	RULES -- 1. sales = price * quantity
	         2. sales and price should not be 0, -ve  or NULL
	         3. if sales - -ve, 0 , null OR sales!= quantity * price  THEN sales = price * quantity
			 4, if price -ve or null THEN price = sales/quantity and quantity must not be 0


	IN DATA -- 1.  price wrongly written -ve -- then make it +ve 


## 9. TRANSFORMATION --

            CASE                                                                                        -- if -VE OR 0 or null sales THEN sales = price * quantity
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)  -- PRICE ABSOLUTE as attime sin dat it is wrongly written -ve
					THEN sls_quantity * ABS(sls_price)                               -- if -VE OR 0 or null sales THEN sales = price * quantity
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0                               --- if price -ve or null THEN price = sales/quantity and quantity must not be 0
					THEN sls_sales / NULLIF(sls_quantity, 0)                           -- to make quantity != 0 with null - replace 0 -- NULLIF(sls_quantity, 0)  
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price

*/





/*
=============================================================================
### APPLYING SCD --

       SCD 0 -- NO CHANGES 
       SCD 1 -- OVERWRITE EXISTING DATA 
	   SCD 2 -- PRESERVE FULL HISTORY
	   SCD 3 -- STORE CURRENT + PREVIOUS

## SCD 0 -
-- Instead of INSERT, use INSERT WHERE NOT EXISTS
INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname,
    cst_marital_status, cst_gndr, cst_create_date
)
SELECT ...
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1
AND NOT EXISTS (
    SELECT 1 FROM silver.crm_cust_info s WHERE s.cst_id = t.cst_id
);


## SCD 1 - code below 

## SCD 2-- 
MERGE silver.crm_cust_info AS target
USING (
    SELECT
        ... -- SAME AS BELOW CODE 
    FROM (
        ... -- SAME CODE  
    ) t
    WHERE flag_last = 1
) AS source
ON target.cst_id = source.cst_id AND target.is_current = 1
WHEN MATCHED AND (
        target.cst_firstname != source.cst_firstname OR
        target.cst_lastname != source.cst_lastname OR
        target.cst_marital_status != source.cst_marital_status OR
        target.cst_gndr != source.cst_gndr
) THEN
    -- Close old record
    UPDATE SET target.effective_end_date = GETDATE(), target.is_current = 0
WHEN NOT MATCHED BY TARGET THEN
    -- Insert new record
    INSERT (cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date,
            effective_start_date, effective_end_date, is_current)
    VALUES (source.cst_id, source.cst_key, source.cst_firstname, source.cst_lastname,
            source.cst_marital_status, source.cst_gndr, source.cst_create_date,
            GETDATE(), NULL, 1);

## SCD 3 --
MERGE silver.crm_cust_info AS target
USING (
    SELECT
       ....
    FROM (
        ....
    ) t
    WHERE flag_last = 1
) AS source
ON target.cst_id = source.cst_id
WHEN MATCHED AND target.cst_marital_status != source.cst_marital_status THEN
    UPDATE SET
        target.prev_marital_status = target.cst_marital_status,
        target.cst_marital_status = source.cst_marital_status,
        target.cst_firstname = source.cst_firstname,
        target.cst_lastname = source.cst_lastname,
        target.cst_gndr = source.cst_gndr,
        target.cst_create_date = source.cst_create_date
WHEN NOT MATCHED THEN
    INSERT (cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, prev_marital_status, cst_gndr, cst_create_date)
    VALUES (source.cst_id, source.cst_key, source.cst_firstname, source.cst_lastname,
            source.cst_marital_status, NULL, source.cst_gndr, source.cst_create_date);

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,    -- To remove unwanted SPACES -- TRIM(column_name) -> Transformation 
			TRIM(cst_lastname) AS cst_lastname,

			CASE                                     -- case -- Data Standardization and consistency -- standard --only single and married , upper and trim used  
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a' -- handling missing values 
			END AS cst_marital_status, -- Normalize marital status values to readable format

			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format

			cst_create_date

        -- To remove NULL and DUPLICATES -- subquery in FROM () -- 
		FROM (
			SELECT                                                                                 -- Transformation happens in select
				*,                                                                                 -- ROW NUMBER AS - flag_last 
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last -- partitionBy- id means - same id wale ek partition mein ,  
			FROM bronze.crm_cust_info                                                              --  then order these by date in desc -- latest on top
			WHERE cst_id IS NOT NULL                                                               -- Always use Row_no. as -- rowno.() OVER(order by or partiton by then orderby ) 
		) t      -- this t is the alias we give after writing subquery                             -- and then AS name -- on which condition is there 
		
		WHERE flag_last = 1; -- Select the most recent record per customer    -- on alias of ROW_number() flag_last u can put condition outide of subquery only
                                                                              -- as WHERE is a condition tranformation and runs before the row_number in query 
																			  -- "execution flow" . Therefore it is declared outise subquery.
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,  -- dervied column as in bronze.crm_prd_info table only prd_key was there from which these both are dervied
			prd_key, -- derived column
			prd_nm,  -- name - handle unwanted spaces 
			prd_cost, -- cost should not be negative 
			prd_line,      -- data standardization/normalization
			prd_start_dt,  -- as hh:mm:ss was just 0 therefore we changed DATA type from datetime to date
			prd_end_dt     -- data enrichment - we deriverd ourselevs - end_date of current colum = start_date -1 of next column
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID -- replace(string , thing to replace , with whom it would be replaced)
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,                       -- if prd_cost 0 make it null , replace 0 with null 
			CASE                                      -- Data Standardization
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values -- END AS ke baad name of colunn jisme ye value jayengi
			CAST(prd_start_dt AS DATE) AS prd_start_dt, -- convert into date datatype
			CAST(                                                                         -- Transformation - called Data Enrichment -- assinging new date
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1  -- lead and lag window function lead(col, offset = 1 by default)
				AS DATE                                 -- end date of current row = start date of next row - 1 day - so no overlapping b/w dates of current and next row 
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,           -- check for unwanted spaces -- as it is a string  eg - SO43697 
			sls_prd_key,           -- check if all the same key exist in other table column which we will join with this table --
			sls_cust_id,
                                                           /*   (** if dates are given as integer -- 20101229  --- 2010 year , 12 month , 29 date) 
	                                                            RULES -- 1. order date -- -ve <=0 --(if -ve or 0) THEN NULL
	                                                                     2. order date length != 8                THEN NULL
                                                                         3. AS date format is wrong convert it -- into date data type 
															*/
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL           -- make null if -ve or 0   
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)                    -- convert data type  
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
            
			CASE                                                                                      
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)  -- PRICE ABSOLUTE as attime sin dat it is wrongly written -ve
					THEN sls_quantity * ABS(sls_price)                               -- if -VE OR 0 or null sales THEN sales = price * quantity
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0                               --- if price -ve or null THEN price = sales/quantity and quantity must not be 0
					THEN sls_sales / NULLIF(sls_quantity, 0)                           -- to make quantity != 0 with null - replace 0 -- NULLIF(sls_quantity, 0)  
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
    
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        ---------------------------------------------------------------------------------
        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		

		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
