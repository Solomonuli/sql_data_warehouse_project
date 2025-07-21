/*This is the part of the project where we clean and standardize our data by checking for duplicates, nulls,
and ensure consistency of the data across all records*/

-- Below are the data cleaning codes for all 6 tables done 

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

---------------------------------------------------------------------------------------------------------------------------------------------

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


---------------------------------------------------------------------------------------------------------------------------------------------


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

--------------------------------------------------------------------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------------------------------------------------------------------

TRUNCATE erp_px_cat_g1v2;

INSERT INTO erp_px_cat_g1v2
SELECT *
FROM erp_px_cat_g1v23































































