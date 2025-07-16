/* 
-----------------------------------------------------------------------------------------------------------------------------
**DDL SCRIPT ; Inputing Data To Tables**
----------------------------------------------------------------------------------------------------------------------------

Due to some difficulties while importing in MySQL data using QUERY METHOD, I had to import using the navigation bar on 
the left MySQL Workbench's Navigator/Data Import GUI.

I then proceeded to alter some information after importation as below.

*/

ALTER TABLE cust_info
MODIFY COLUMN cst_key VARCHAR(50),
MODIFY COLUMN cst_firstname VARCHAR(50),
MODIFY COLUMN cst_lastname VARCHAR(50),
MODIFY COLUMN cst_marital_status VARCHAR(50),
MODIFY COLUMN cst_gndr VARCHAR(50),
MODIFY COLUMN cst_create_date DATE;


ALTER TABLE prd_info
MODIFY COLUMN prd_key VARCHAR(50),
MODIFY COLUMN prd_nm VARCHAR(50),
-- MODIFY COLUMN prd_cost INT,
MODIFY COLUMN prd_line VARCHAR(50),
MODIFY COLUMN prd_start_dt DATETIME;
-- MODIFY COLUMN prd_end_dt DATETIME;

ALTER TABLE prd_info
MODIFY COLUMN prd_end_dt DATETIME NULL;

SHOW CREATE TABLE prd_info;

ALTER TABLE sales_details
MODIFY COLUMN sls_ord_num VARCHAR(50),
MODIFY COLUMN sls_prd_key VARCHAR(50);


RENAME TABLE cust_info TO crm_cust_info;
RENAME TABLE prd_info TO crm_prd_info;
RENAME TABLE sales_details TO crm_sales_details;


ALTER TABLE px_cat_g1v2
MODIFY COLUMN ID VARCHAR(50),
MODIFY COLUMN CAT VARCHAR(50),
MODIFY COLUMN SUBCAT VARCHAR(50),
MODIFY COLUMN MAINTENANCE VARCHAR(50);

RENAME TABLE px_cat_g1v2 TO erp_px_cat_g1v2;

ALTER TABLE cust_az12
MODIFY COLUMN CID VARCHAR(50),
MODIFY COLUMN BDATE DATE,
MODIFY COLUMN GEN VARCHAR(50);

RENAME TABLE cust_az12 TO erp_cust_az12;

ALTER TABLE loc_a101
MODIFY COLUMN CID VARCHAR(50),
MODIFY COLUMN CNTRY VARCHAR(50);

RENAME TABLE loc_a101 TO erp_loc_a101;
