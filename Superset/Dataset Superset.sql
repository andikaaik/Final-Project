-- =========================
-- DATASET PRODUCT
-- =========================
SELECT
    dp.product_name AS Product,
    dp.category_name,
    dt.month_name,
    dt.year,
    COALESCE(SUM(fs.quantity), 0) AS total_quantity,
    COALESCE(SUM(fs.total_price), 0) AS total_revenue,
    COUNT(fs.sales_id) AS total_transaksi
FROM project_dwh.dim_product dp
LEFT JOIN project_dwh.fact_sales fs 
    ON dp.sk_product = fs.sk_product
LEFT JOIN project_dwh.dim_time dt 
    ON fs.sk_date = dt.sk_date
GROUP BY
    dp.product_name,
    dp.category_name,
    dt.month_name,
    dt.year
    ;

-- =========================
-- DATASET CUSTOMER
-- =========================
SELECT
    dc.sk_customer,
    dc.customer_id,
    dc.customer_first_name || ' ' || dc.customer_last_name AS Name,
    dc.customer_address,
    dc.customer_city_name,
    dc.customer_zipcode,
    dc.customer_country_name,
    dc.customer_country_code,

    COALESCE(SUM(fs.total_price), 0) AS total_revenue,

    COALESCE(SUM(fs.quantity), 0) AS total_quantity,
    COUNT(fs.sales_id) AS total_transaksi,
    COALESCE(AVG(fs.total_price), 0) AS avg_order_value,

    dt.month_name,
    dt.year
FROM project_dwh.dim_customer dc
LEFT JOIN project_dwh.fact_sales fs 
    ON dc.sk_customer = fs.sk_customer
LEFT JOIN project_dwh.dim_time dt 
    ON fs.sk_date = dt.sk_date

GROUP BY
    dc.sk_customer, dc.customer_id,
    dc.customer_first_name, dc.customer_last_name,
    dc.customer_address, dc.customer_city_name,
    dc.customer_zipcode, dc.customer_country_name,
    dc.customer_country_code,
    dt.month_name, dt.year

-- =========================
-- DATASET EMPLOYEE
-- =========================
SELECT
    de.sk_employee,
    de.employee_id,
    de.employee_first_name || ' ' || de.employee_last_name AS Name,
    de.employee_gender,
    de.employee_birth_date,
    de.employee_hire_date,
    de.employee_city_name,
    de.employee_country_name,
    de.employee_country_code,
    SUM(fs.total_price) AS total_revenue,
    SUM(fs.quantity) AS total_quantity,
    COUNT(fs.sales_id) AS total_transaksi,
    dt.month_name,
    dt.year,
    dt.date
FROM project_dwh.dim_employee de
LEFT JOIN project_dwh.fact_sales fs ON de.sk_employee = fs.sk_employee
LEFT JOIN project_dwh.dim_time dt ON fs.sk_date = dt.sk_date
GROUP BY
    de.sk_employee, de.employee_id,
    de.employee_first_name, de.employee_last_name,
    de.employee_gender, de.employee_birth_date,
    de.employee_hire_date, de.employee_city_name,
    de.employee_country_name, de.employee_country_code,
    dt.month_name, dt.year, dt.date
    
-- =========================
-- DATASET SALES
-- =========================
SELECT 
    fs.sales_id,
    fs.transaction_no,
    dt.date,
    dt.days,
    dt.month_name,
    dt.year,
    dc.customer_first_name || ' ' || dc.customer_last_name AS customer_name,
    dc.customer_city_name,
    dc.customer_country_name,
    de.employee_first_name || ' ' || de.employee_last_name AS employee_name,
    dp.product_name,
    dp.product_price,
    dp.category_name,
    fs.quantity,
    fs.discount,
    fs.total_price
FROM project_dwh.fact_sales fs
LEFT JOIN project_dwh.dim_time dt ON fs.sk_date = dt.sk_date
LEFT JOIN project_dwh.dim_customer dc ON fs.sk_customer = dc.sk_customer
LEFT JOIN project_dwh.dim_employee de ON fs.sk_employee = de.sk_employee
LEFT JOIN project_dwh.dim_product dp ON fs.sk_product = dp.sk_product

