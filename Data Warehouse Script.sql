-- =====================================
-- SQL Data Warehouse: Bronze, Silver, Gold Layers
-- =====================================

-- =====================================
-- BRONZE LAYER: Raw data tables
-- =====================================

-- Customer information table
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

-- Product information table
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

-- Sales details table
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- ERP location table
CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- ERP customer table
CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

-- ERP product category table
CREATE TABLE bronze.erp_px_cat_giv2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

-- =====================================
-- BULK LOAD DATA INTO BRONZE TABLES
-- =====================================

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

BULK INSERT bronze.erp_px_cat_giv2
FROM 'C:\Users\placeholder\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- =====================================
-- SILVER LAYER: Cleaned & transformed tables
-- =====================================

-- Customer information with DWH metadata
CREATE TABLE silver.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- Product information with DWH metadata
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- Sales details with DWH metadata
CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- ERP location with DWH metadata
CREATE TABLE silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- ERP customer with DWH metadata
CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- ERP product category with DWH metadata
CREATE TABLE silver.erp_px_cat_giv2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

-- =====================================
-- SILVER LAYER: Data cleaning & inserts
-- =====================================

-- Insert cleaned customer data
INSERT INTO silver.crm_cust_info(
    cst_id, cst_key,
    cst_firstname, cst_lastname,
    cst_material_status, cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_material_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- Insert cleaned product data
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_prd_info(
    prd_id, cat_id, prd_key, prd_nm, prd_cost,
    prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost,0),
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    CAST(prd_start_dt AS DATE),
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)
FROM bronze.crm_prd_info;

-- Clean and insert sales data
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_data DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details(
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END,
    CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END,
    CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END,
    CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price<=0
         THEN sls_sales/NULLIF(sls_quantity,0)
         ELSE sls_price
    END
FROM bronze.crm_sales_details;

-- Insert ERP customer and location data
INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
    CASE WHEN bdate>GETDATE() THEN NULL ELSE bdate END,
    CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
         ELSE 'n/a' END
FROM bronze.erp_cust_az12;

INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT
    REPLACE(cid,'-',''),
    CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
         WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry) END
FROM bronze.erp_loc_a101;

INSERT INTO silver.erp_px_cat_giv2(id,cat,subcat,maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_giv2;

-- =====================================
-- GOLD LAYER: Dimensional Views
-- =====================================

-- Customer dimension view
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_ley,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr ELSE COALESCE(ca.gen,'n/a') END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;

-- Product dimension view
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_giv2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;

-- Sales fact view
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_ley AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;
