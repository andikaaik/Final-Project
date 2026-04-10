-- =========================
-- SCHEMA
-- =========================
CREATE SCHEMA IF NOT EXISTS project_stg;

-- =========================
-- STAGING SALES
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_sales (
    salesid INT,
    salespersonid INT,
    customerid INT,
    productid INT,
    quantity INT,
    discount NUMERIC(6,2),
    totalprice NUMERIC(18,2),
    salesdate DATE,
    transactionnumber TEXT
);

-- =========================
-- STAGING PRODUCTS
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_products (
    productid INT,
    productname TEXT,
    price NUMERIC(18,2),
    categoryid INT,
    class TEXT,
    modifydate TIMESTAMP,
    resistant TEXT,
    isallergic TEXT,
    vitalitydays INT
);

-- =========================
-- STAGING CUSTOMERS
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_customers (
    customerid INT,
    firstname TEXT,
    middleinitial TEXT,
    lastname TEXT,
    cityid INT,
    address TEXT
);

-- =========================
-- STAGING CATEGORIES
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_categories (
    categoryid INT,
    categoryname TEXT
);

-- =========================
-- STAGING EMPLOYEE
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_employee (
    employeeid INT,
    firstname TEXT,
    middleinitial TEXT,
    lastname TEXT,
    birthdate DATE,
    gender TEXT,
    cityid INT,
    hiredate TIMESTAMP
);

-- =========================
-- STAGING CITIES
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_cities (
    cityid INT,
    cityname TEXT,
    zipcode TEXT,
    countryid INT
);

-- =========================
-- STAGING COUNTRIES
-- =========================
CREATE TABLE IF NOT EXISTS project_stg.stg_countries (
    countryid INT,
    countryname TEXT,
    countrycode TEXT
);