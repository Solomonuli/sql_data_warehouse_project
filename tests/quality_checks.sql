/*
=============================================================================================================================================

**Quality Checks**
=============================================================================================================================================
Script Purpose:
    This script shows and performs carious quality checks for data consistency, accuracy and standardization across the database
    before loading the cleaned data.
    It includes checks for :
      1. Null or duplicate primary keys
      2. Unwanted spaces in string fields
      3. Invalid date ranges and orders
      4. Data Consistency between related fields

*/
===============================================================================================================================================

-- Checking crm_cust_info2

==============================================================================================================================================

-- Data Cleaning --
-- we start by checking duplicates generally . We start with table crm_cust_info
SELECT *
FROM crm_cust_info2;

SELECT *,
ROW_NUMBER() OVER(Partition By cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,
cst_gndr, cst_create_date) row_num
FROM crm_cust_info2;

SELECT *
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,
		cst_gndr, cst_create_date) row_num
		FROM crm_cust_info2) t
WHERE row_num > 1;

-- we then check whether the primary key has occurred more than once

SELECT *
FROM crm_cust_info2;

SELECT 
cst_id,
COUNT(*)
FROM crm_cust_info2
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- WE now proceed to check individually where the duplicates exist

SELECT *
FROM crm_cust_info2
WHERE cst_id = 29466;      -- here, we are only interested in one value of the 3 that appear
						   -- we pick one value by checking the date value/time stamp value that is current as it holds the freshest info
                           -- hence we rank the values based on the create dates and pick the highest one
                           
SELECT *,
ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
FROM crm_cust_info2
WHERE cst_id = 29466;

-- we can also check the whole table generally

SELECT *,
ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
FROM crm_cust_info2;

SELECT *
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top != 1;

SELECT *
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top = 1;

-- we have now separated the duplicates with those queries

-- we next check values for unwanted spaces

SELECT *
FROM crm_cust_info2;

SELECT 
cst_firstname,
TRIM(cst_firstname) Proper
FROM crm_cust_info2;            -- This shows everything

SELECT 
cst_firstname
FROM crm_cust_info2
WHERE cst_firstname != TRIM(cst_firstname);  -- This only shows the ones with unwanted spaces

SELECT 
cst_lastname,
TRIM(cst_lastname) Proper
FROM crm_cust_info2;

SELECT 
cst_lastname
FROM crm_cust_info2
WHERE cst_lastname != TRIM(cst_lastname);

-- we now incorporte our findings 

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top = 1;

/*   next, we want to check the consistency of values in low cardinality(Column cardinality indicates 
the number of unique values in a column relative to the total number of rows in the table. )

A column with high cardinality has many distinct values (e.g., a customer ID column where each ID is unique). 
A column with low cardinality has a small number of distinct values (e.g., a gender column with only "Male" and "Female"). 
*/

-- Data standardization and Consistency

SELECT DISTINCT cst_gndr
FROM crm_cust_info2;     -- in our data warehouse, we aim to store data with clear and meaningful values rather than abbreviations.

-- we then transform the information in the cst_gndr column to full names using CASE STATEMENT
-- and incorporate it to the cleanup query

 
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
CASE WHEN cst_gndr = 'M' THEN 'Male'
	 WHEN cst_gndr = 'F' THEN 'Female'
     ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top = 1;

-- other additions can be made such as capitalization and also trim

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top = 1;

-- WE then repeat the process for the cst_marital_status column

SELECT DISTINCT cst_marital_status
FROM crm_cust_info2;

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     ELSE 'N/A'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM ( SELECT *,
		ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
		FROM crm_cust_info2) T
WHERE flag_top = 1;

-- the table is finally cleaned, we need to put it in another table but i'll format the first table and put the cleaned format there

TRUNCATE crm_cust_info;

INSERT INTO crm_cust_info
SELECT *
FROM (
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'N/A'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 ELSE 'N/A'
			END cst_gndr,
			cst_create_date
		FROM ( SELECT *,
				ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) AS flag_top
				FROM crm_cust_info2) T
				WHERE flag_top = 1
              ) y;

SELECT *
FROM crm_cust_info;

=========================================================================================================================================

-- Checking crm_prd_info2

=========================================================================================================================================

-- lets check for duplicates in the primary key

SELECT *
FROM crm_prd_info2;

SELECT 
prd_id,
COUNT(*)
FROM crm_prd_info2
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;           -- we dont have duplicates in the primary key


-- we then check the product key
-- but we have to split the product key because the first 4 letters match the category id from the category table

SELECT DISTINCT ID
FROM erp_px_cat_g1v23;

SELECT 
prd_id,
prd_key,
SUBSTRING(prd_key,1,5) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

-- we then replace the hyphen on the cat_id with an underscore thats like in the category table

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

-- We now proceed to split the second part of the product key

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

select * from crm_sales_details2;

-- moving on to the next column,product name, we check whether we might have spaces

SELECT *
FROM crm_prd_info2
WHERE prd_nm != TRIM(prd_nm);      -- the data is without spaces

-- we then check the next column, product cost, whether it has nulls or negative numbers

SELECT prd_cost
FROM crm_prd_info2
WHERE prd_cost IS NULL OR prd_cost = '' ;   -- we found 2 empty strings, so we replace them with 0


-- going back to our main query 

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
CASE WHEN prd_cost = '' THEN 0
	 ELSE prd_cost
END AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

-- Data standardization and Consistency
-- we check the next column esp since its is using an abbreviation and has low cardinality

SELECT DISTINCT prd_line
FROM crm_prd_info2;

-- note that since we dont know the values of the abbreviations, we have to ask expert from the source 

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
CASE WHEN prd_cost = '' THEN 0
	 ELSE prd_cost
END AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

-- the above formula can be done in a different way to avoid repetition esp if the value is the same and not complex

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
IF(prd_cost= '', 0, prd_cost) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
     WHEN 'M' THEN 'Mountain'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2;

-- in the last 2 columns , we check for invalid date orders

SELECT *
FROM crm_prd_info2;

SELECT *
FROM crm_prd_info2
WHERE prd_end_dt < prd_start_dt;   

/*
The end date must not be earlier then start date and should avoid date overlaps and nulls in the start date.
We therefore decide to drop the end date and use the start dates to derive the end dates. Ofcourse after approval from the source. 

The formula required for this is the Window function known as LEAD.
We will pick out 2 product keys to test the theory

*/

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt
FROM crm_prd_info2
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER(Partition By prd_key Order By prd_start_dt) AS prd_end_dt_TEST
FROM crm_prd_info2
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- seeing thatthe dates are ok, we go to the test end end to derive the previous day by Subtracting 1

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
DATE_SUB(LEAD(prd_start_dt) OVER(Partition By prd_key Order By prd_start_dt),INTERVAL 1 DAY) AS prd_end_dt_TEST
FROM crm_prd_info2
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- now, on the dates, we can see time information but they are blank. we can therefore choose to remove them
-- we also replace the previous end time with the new end time

SELECT
prd_id,
prd_key,
prd_nm,
DATE(prd_start_dt) AS pdr_start_dt,
DATE(DATE_SUB(LEAD(prd_start_dt) OVER(Partition By prd_key Order By prd_start_dt),INTERVAL 1 DAY)) AS prd_end_dt
FROM crm_prd_info2
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- after this, we incorporate the formula in the main query for data transformation.

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
IF(prd_cost= '', 0, prd_cost) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
     WHEN 'M' THEN 'Mountain'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
DATE(prd_start_dt) AS pdr_start_dt,
DATE(DATE_SUB(LEAD(prd_start_dt) OVER(Partition By prd_key Order By prd_start_dt),INTERVAL 1 DAY)) AS prd_end_dt
FROM crm_prd_info2;

-- we then insert the cleaned data to another table for clarity, in this case crm_prd_info

SHOW CREATE TABLE crm_prd_info2;

CREATE TABLE crm_prd_info (
	prd_id INT,
    cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

TRUNCATE crm_prd_info;
INSERT INTO crm_prd_info
SELECT *
FROM (
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
		prd_nm,
		IF(prd_cost= '', 0, prd_cost) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		DATE(prd_start_dt) AS pdr_start_dt,
		DATE(DATE_SUB(LEAD(prd_start_dt) OVER(Partition By prd_key Order By prd_start_dt),INTERVAL 1 DAY)) AS prd_end_dt
		FROM crm_prd_info2
)t ;

SELECT *
FROM crm_prd_info;

==========================================================================================================================================

-- Checking crm_sales_details2
==========================================================================================================================================


SELECT *
FROM crm_sales_details2;


SELECT *
FROM crm_sales_details2
WHERE sls_ord_num != TRIM(sls_ord_num);        -- the first column looks clean

-- we then check whether the next 2 columns are connectable to the other tables
-- its always good to select all columns instead of using *

SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
sls_order_dt, 
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
FROM crm_sales_details2
WHERE sls_prd_key NOT IN (SELECT prd_key FROM crm_prd_info);   -- this means that there is no alien value in sales details table.
															   -- they are linked

SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
sls_order_dt, 
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
FROM crm_sales_details2
WHERE sls_cust_id NOT IN (SELECT cst_id FROM crm_cust_info);   -- this means that there is no alien value in sales details table.
															   -- they are linked


-- the next 3 columns seem to be dates but written as integers. 
-- we first start checking for invalid dates

SELECT sls_order_dt
FROM crm_sales_details2
WHERE sls_order_dt <= 0;

-- we will proceed to change the values to null. We use nullif function

SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM crm_sales_details2
WHERE sls_order_dt <= 0;

-- Checking the lenght of the sales order it, the digits are 8, so we check whether there is a value to the contrary
-- This means checking for outliers by validating the boundaries of the date range 

SELECT sls_order_dt
FROM crm_sales_details2
WHERE LENGTH(sls_order_dt) != 8;

SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM crm_sales_details2
WHERE LENGTH(sls_order_dt) != 8 ;


-- we therefore proceed to make changes in the main query

SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')   -- ensure to check from the original data how the date is arranged
 END AS sls_order_dt,
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
FROM crm_sales_details2;

-- we will make the same checks and changes to the sls_shp_dt and sls_due_dt

SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')   
 END AS sls_order_dt,
 CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')   
 END AS sls_ship_dt, 
 CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')   
 END AS sls_due_dt,
sls_sales, 
sls_quantity, 
sls_price
FROM crm_sales_details2;

-- another check is to see whether the order date precedes the due date and ship date 

SELECT *
FROM crm_sales_details2
WHERE sls_due_dt < sls_order_dt 
OR sls_ship_dt < sls_order_dt;

/* 
lastly we check the last 3 columns. according to business rules, 
 Total Sales = Quantity * Price
 
 This means that all sales should have a +VE Value , not negative, zeros, or nulls
 
 */
 
SELECT
 sls_sales,
 sls_quantity,
 sls_price
FROM crm_sales_details2
WHERE sls_sales != sls_quantity * sls_price
 OR sls_sales IS NULL
 OR sls_quantity IS NULL
 OR sls_price IS NULL
 OR sls_sales <= 0
 OR sls_quantity <= 0
 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price;

/*  seeing all those errors, we need to consult the owner of the data.
The issues can either be solved from the source system, or we can go ahead and solve them ourselves. 
 
 Rules to follow in order to solve the issues
1. If sales is negative, zero or null, we derive it using quantity and price
2. If price is zero or null, we calculate it using Sales and Quantity
3. If price is negative, convert it to a positive value. 

*/

SELECT
 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	  THEN sls_quantity * ABS(sls_price)    
 ELSE sls_sales
 END AS sls_sales,
 sls_quantity,
 CASE WHEN sls_price IS NULL OR sls_price <= 0
	  THEN sls_sales/ sls_quantity 
 ELSE sls_price
 END AS sls_price
FROM crm_sales_details2
WHERE sls_sales != sls_quantity * sls_price
 OR sls_sales IS NULL
 OR sls_quantity IS NULL
 OR sls_price IS NULL
 OR sls_sales <= 0
 OR sls_quantity <= 0
 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price;

-- the ABS turns a negative number to a positve one
-- we can do sls_sale/ nullif(sls_quantity,0) in cases of a zero on the data

-- by doing the case statements, we have cleaned up the data in those 3 columns.
-- we therefore go and integrate it in our main query

SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')   
 END AS sls_order_dt,
 CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')   
 END AS sls_ship_dt, 
 CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')   
 END AS sls_due_dt,
 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	  THEN sls_quantity * ABS(sls_price)   
 ELSE sls_sales
 END AS sls_sales,
  sls_quantity,
 CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/ sls_quantity  
	  ELSE sls_price
 END AS sls_price
FROM crm_sales_details2;

DROP TABLE crm_sales_details;

CREATE TABLE crm_sales_details(
	sls_ord_num VARCHAR(50),
	  sls_prd_key VARCHAR(50),
	  sls_cust_id INT,
	  sls_order_dt DATE,
	  sls_ship_dt DATE,
	  sls_due_dt DATE,
	  sls_sales INT,
	  sls_quantity INT,
	  sls_price INT
);

TRUNCATE crm_sales_details;

INSERT INTO crm_sales_details
SELECT *
FROM (
	SELECT 
sls_ord_num,
sls_prd_key, 
sls_cust_id, 
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')   
 END AS sls_order_dt,
 CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')   
 END AS sls_ship_dt, 
 CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
	 ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')   
 END AS sls_due_dt,
 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)     
      ELSE sls_sales
 END AS sls_sales,
  sls_quantity,
 CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/ sls_quantity  
      ELSE sls_price
 END AS sls_price
FROM crm_sales_details2
) t ;

SELECT * FROM crm_sales_details;

==========================================================================================================================================

-- Checking erp_cust_az123
==========================================================================================================================================


-- we can start by checking whether the 1st column is connectable to the other tables


SELECT
CID,
BDATE,
GEN
FROM erp_cust_az123;

SELECT *
FROM crm_cust_info2;

-- we can clearly see that the customer IDS of both tables are somewhat connected but the erp table needs to be cleaned


SELECT
CID,
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END CID,
BDATE,
GEN
FROM erp_cust_az123;

-- We then check the birth date. We check whether there are anomaly dates where the birthdate is greater than the current date

SELECT
BDATE
FROM erp_cust_az123
WHERE BDATE > current_date();

-- seeing that there are anomalies, we use a case statement in the main query to eliminate the error

SELECT
CID,
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END CID,
CASE WHEN BDATE > current_date() THEN NULL
	 ELSE BDATE
END BDATE,
GEN
FROM erp_cust_az123;

-- We next standardize the gender column

SELECT DISTINCT GEN
FROM erp_cust_az123;

-- this is how i tried doing it but it was wrong

/* SELECT
CID,
BDATE,
CASE WHEN GEN = 'M' THEN 'Male'
	 WHEN GEN = 'F' THEN 'Female'
     WHEN GEN = '' THEN 'NULL'
     ELSE GEN
END GEN
FROM erp_cust_az123;

SELECT DISTINCT GEN
FROM (
	SELECT
CID,
BDATE,
CASE WHEN GEN = 'M' THEN 'Male'
	 WHEN GEN = 'F' THEN 'Female'
     WHEN GEN = '' THEN 'NULL'
     ELSE GEN
END GEN
FROM erp_cust_az12
) y ;

*/

-- how to do it correctly

SELECT DISTINCT GEN,
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/a'
END GEN
FROM erp_cust_az123;

-- we therefore incorporate it in our main query

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END CID,
CASE WHEN BDATE > current_date() THEN NULL
	 ELSE BDATE
END BDATE,
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/a'
END GEN
FROM erp_cust_az123;


SELECT *
FROM erp_cust_az12;

TRUNCATE erp_cust_az12;

INSERT INTO erp_cust_az12
SELECT *
FROM (
SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END CID,
CASE WHEN BDATE > current_date() THEN NULL
	 ELSE BDATE
END BDATE,
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/a'
END GEN
FROM erp_cust_az123
) t ;


SELECT *
FROM erp_cust_az12;
==========================================================================================================================================

-- checking erp_loc_a1013
==========================================================================================================================================
  
SELECT *
FROM erp_loc_a1013;

-- the cid has an unnecessary hyphen . we go ahead and remove it

SELECT
CID,
REPLACE(CID, '-', '') CID,
CNTRY
FROM erp_loc_a1013;

-- we can check for distinct countries available

SELECT DISTINCT CNTRY
FROM erp_loc_a1013
ORDER BY CNTRY;

-- the column needs to be standardized for better information

SELECT DISTINCT CNTRY,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR CNTRY IS NULL THEN 'N/a'
ELSE TRIM(CNTRY)
END CNTRY
FROM erp_loc_a1013;

-- we then incorporate the transformation to the main query

SELECT
REPLACE(CID, '-', '') CID,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR CNTRY IS NULL THEN 'N/a'
ELSE TRIM(CNTRY)
END CNTRY
FROM erp_loc_a1013;

-- as usual, we copy that information to  a nw table


SELECT *
FROM erp_loc_a101;

TRUNCATE erp_loc_a101;

INSERT INTO erp_loc_a101
SELECT *
FROM (
	SELECT
		REPLACE(CID, '-', '') CID,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR CNTRY IS NULL THEN 'N/a'
		ELSE TRIM(CNTRY)
END CNTRY
FROM erp_loc_a1013
) t ;

SELECT *
FROM erp_loc_a101;

==========================================================================================================================================
-- checkingerp_px_cat_g1v23
==========================================================================================================================================
  
SELECT *
FROM erp_px_cat_g1v23;

SELECT *
FROM crm_prd_info;

SELECT
ID,
CAT,
SUBCAT,
MAINTENANCE
FROM erp_px_cat_g1v23
WHERE ID NOT IN (SELECT cat_id FROM crm_prd_info);

-- we check for standardization
SELECT DISTINCT CAT
FROM erp_px_cat_g1v23;

SELECT DISTINCT SUBCAT
FROM erp_px_cat_g1v23;

SELECT DISTINCT MAINTENANCE
FROM erp_px_cat_g1v23;


-- WE check for unwanted spaces
SELECT *
FROM erp_px_cat_g1v23
WHERE CAT != TRIM(CAT)
OR SUBCAT != TRIM(SUBCAT)
OR MAINTENANCE != TRIM(MAINTENANCE);

-- everything looks up to standar in this table

SELECT *
FROM erp_px_cat_g1v2;

TRUNCATE erp_px_cat_g1v2;

INSERT INTO erp_px_cat_g1v2
SELECT *
FROM erp_px_cat_g1v23;















