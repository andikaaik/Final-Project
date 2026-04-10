-- =========================
-- SCHEMA
-- =========================
CREATE SCHEMA IF NOT EXISTS project_dwh;

-- =========================
-- DIM PRODUCT
-- =========================
CREATE TABLE IF NOT EXISTS project_dwh.dim_product (
    sk_product SERIAL PRIMARY KEY,
    product_id INT,
    product_name TEXT,
    product_price NUMERIC(18,2),
    category_id INT,
    category_name TEXT,
    modify_datetime TIMESTAMP,
    insert_date DATE DEFAULT CURRENT_DATE
);

-- =========================
-- DIM CUSTOMER
-- =========================
CREATE TABLE IF NOT EXISTS project_dwh.dim_customer (
    sk_customer SERIAL PRIMARY KEY,
    customer_id INT,
    customer_first_name TEXT,
    customer_middle_initial_name TEXT,
    customer_last_name TEXT,
    customer_address TEXT,
    customer_city_id INT,
    customer_city_name TEXT,
    customer_zipcode TEXT,
    customer_country_id INT,
    customer_country_name TEXT,
    customer_country_code TEXT,
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE
);

-- =========================
-- DIM EMPLOYEE
-- =========================
CREATE TABLE IF NOT EXISTS project_dwh.dim_employee (
    sk_employee SERIAL PRIMARY KEY,
    employee_id INT,
    employee_first_name TEXT,
    employee_middle_initial TEXT,
    employee_last_name TEXT,
    employee_birth_date DATE,
    employee_gender TEXT,
    employee_city_id INT,
    employee_city_name TEXT,
    employee_country_id INT,
    employee_country_name TEXT,
    employee_country_code TEXT,
    employee_hire_date TIMESTAMP,
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE
);

-- =========================
-- DIM TIME
-- =========================
CREATE TABLE IF NOT EXISTS project_dwh.dim_time (
    sk_date SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    days VARCHAR(40),
    month_id INT,
    month_name VARCHAR(40),
    year INT
);

-- =========================
-- DIM SALES
-- =========================
CREATE TABLE IF NOT EXISTS project_dwh.fact_sales (
    sk_date INT,
    sk_customer INT,
    sk_employee INT,
    sk_product INT,
    sales_id INT,
    transaction_no TEXT,
    quantity NUMERIC(18,2),
    discount NUMERIC(6,2),
    total_price NUMERIC(18,2),
    insert_date DATE DEFAULT CURRENT_DATE
);