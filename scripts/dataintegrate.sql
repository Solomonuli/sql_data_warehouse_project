/*
===============================================================================
DDL Script: Create Views
===============================================================================
Script Purpose:
    This script creates views for the Final layer in the data warehouse. 
    The Final layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension Table: dim_customers
-- =============================================================================
DROP VIEW IF EXISTS dim_customers;
CREATE VIEW dim_customers AS 
		( SELECT 
					ROW_NUMBER() OVER(Order By cst_id) AS customer_key,  -- Surrogate key
					ci.cst_id AS customer_id,
					ci.cst_key AS customer_number,
					ci.cst_firstname AS first_name,
					ci.cst_lastname AS last_name,
					MAX(la.CNTRY) AS country,
					ci.cst_marital_status AS marital_status,
					CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr              -- CRM is the primary source for gender
						  ELSE Coalesce(ca.GEN, 'N/A')                                   -- Fallback to ERP data
					END AS gender,
					MAX(ca.BDATE) AS birth_date,
					ci.cst_create_date AS create_date
		FROM crm_cust_info AS ci
		LEFT JOIN erp_cust_az12 AS ca
		ON ci.cst_key = ca.CID
		LEFT JOIN erp_loc_a101 AS la
		ON ci.cst_key = la.CID
		GROUP BY ci.cst_id,
			ci.cst_key,
			ci.cst_firstname,
			ci.cst_lastname,
			ci.cst_marital_status,
			gender,
			ci.cst_create_date)
            ;

-- =============================================================================
-- Create Dimension Table: dim_products
-- =============================================================================
DROP VIEW IF EXISTS dim_products;
CREATE VIEW dim_products AS
	(
SELECT 
	ROW_NUMBER() OVER(Order By pn.prd_start_dt, pn.prd_key) AS product_key,  -- Surrogate key
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS sub_category,
	pc.MAINTENANCE AS maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM crm_prd_info AS pn
LEFT JOIN erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.ID
WHERE pn.prd_end_dt IS NULL  -- Filters out all historical data
	) ;

-- =============================================================================
-- Create Fact Table: fact_sales
-- =============================================================================


DROP VIEW IF EXISTS fact_sales;
CREATE VIEW fact_sales AS
	(
		SELECT
			sls_ord_num AS order_number,
			dp.product_key,
			dc.customer_key,
			sls_order_dt AS order_date,
			sls_ship_dt  AS shipping_date,
			sls_due_dt AS due_date,
			sls_sales AS sales_amont,
			sls_quantity AS quantity,
			sls_price AS price
		FROM  crm_sales_details AS sd
		LEFT JOIN dim_customers AS dc
		ON sd.sls_cust_id = dc.customer_id
		LEFT JOIN dim_products AS dp
		ON sd.sls_prd_key = dp.product_number
        )
;









