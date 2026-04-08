from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch


# LOAD CSV → STAGING

def load_all_staging():

    conn = psycopg2.connect(
        "postgresql://neondb_owner:npg_3y6UGZXhBVLk@ep-steep-bird-a1oa2rfn-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")
    cur = conn.cursor()

    # SALES
    df1 = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.sales.csv", sep=';')
    df2 = pd.read_csv("/opt/airflow/dags/input/20180509/20180509.sales.csv", sep=';')
    sales = pd.concat([df1, df2], ignore_index=True)
    sales.columns = sales.columns.str.strip().str.lower()

    # mengubah salesdate dari integer (20180508) menjadi format tanggal (YYYY-MM-DD)
    sales['salesdate'] = pd.to_datetime(
        sales['salesdate'], format='%Y%m%d', errors='coerce'
    ).dt.date

    # menangani data kosong (NaN). 
    # Ganti NaN di kolom numerik dengan 0
    sales['discount'] = sales['discount'].fillna(0)
    sales['totalprice'] = sales['totalprice'].fillna(0)

    cur.execute("TRUNCATE project_stg.stg_sales;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_sales (
            salesid, salespersonid, customerid, productid,
            quantity, discount, totalprice, salesdate, transactionnumber
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, sales[[
        'salesid','salespersonid','customerid','productid',
        'quantity','discount','totalprice','salesdate','transactionnumber'
    ]].values.tolist())


    # PRODUCTS
    products = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.products.csv", sep=';')
    products.columns = products.columns.str.strip().str.lower()
    products['modifydate'] = pd.to_datetime(
    products['modifydate'], errors='coerce')
    products['price'] = products['price'].astype(str).str.replace(',', '.').astype(float)
    products['price'] = products['price'].fillna(0)

    cur.execute("TRUNCATE project_stg.stg_products;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_products (
            productid, productname, price, categoryid,
            class, modifydate, resistant, isallergic, vitalitydays
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, products[[
        'productid','productname','price','categoryid',
        'class','modifydate','resistant','isallergic','vitalitydays'
    ]].values.tolist())


    # CUSTOMERS
    customers = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.customers.csv", sep=';')
    customers.columns = customers.columns.str.strip().str.lower()

    cur.execute("TRUNCATE project_stg.stg_customers;")
    
    execute_batch(cur, """
        INSERT INTO project_stg.stg_customers (
            customerid, firstname, middleinitial, lastname, cityid, address
        )
    VALUES (%s,%s,%s,%s,%s,%s)
    """, customers[[
        'customerid','firstname','middleinitial','lastname','cityid','address'
    ]].values.tolist())

    # CATEGORIES
    categories = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.categories.csv", sep='|')
    categories.columns = categories.columns.str.strip().str.lower()

    cur.execute("TRUNCATE project_stg.stg_categories;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_categories (
            categoryid, categoryname
        )
    VALUES (%s,%s)
    """, categories[[
        'categoryid','categoryname'
    ]].values.tolist())

    # EMPLOYEE
    employee = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.employes.csv", sep=';')
    employee.columns = employee.columns.str.strip().str.lower()
    employee['birthdate'] = pd.to_datetime(employee['birthdate'], errors='coerce')
    employee['hiredate'] = pd.to_datetime(employee['hiredate'], errors='coerce')

    cur.execute("TRUNCATE project_stg.stg_employee;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_employee (
            employeeid, firstname, middleinitial, lastname,
            birthdate, gender, cityid, hiredate
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, employee[[
        'employeeid','firstname','middleinitial','lastname',
        'birthdate','gender','cityid','hiredate'
    ]].values.tolist())

    # CITIES
    cities = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.cities.csv", sep=';')
    cities.columns = cities.columns.str.strip().str.lower()

    cur.execute("TRUNCATE project_stg.stg_cities;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_cities (
            cityid, cityname, zipcode, countryid
        )
        VALUES (%s,%s,%s,%s)
    """, cities[[
        'cityid','cityname','zipcode','countryid'
    ]].values.tolist())


    # COUNTRIES
    countries = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.countries.csv", sep=';')
    countries.columns = countries.columns.str.strip().str.lower()

    cur.execute("TRUNCATE project_stg.stg_countries;")

    execute_batch(cur, """
        INSERT INTO project_stg.stg_countries (
            countryid, countryname, countrycode
        )
    VALUES (%s,%s,%s)
    """, countries[[
        'countryid','countryname','countrycode'
    ]].values.tolist())


    print(f"Sales loaded: {len(sales)}")
    print(f"Products loaded: {len(products)}")
    print(f"Customers loaded: {len(customers)}")

    conn.commit()
    cur.close()
    conn.close()

#===============
# DAG

with DAG(
    'andika_pipeline_final',
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    # LOAD STAGING
    load_task = PythonOperator(
        task_id='load_all_staging',
        python_callable=load_all_staging
    )

    # DIM TIME (GENERATE)
    dim_time = SQLExecuteQueryOperator(
        task_id='dim_time',
        conn_id='neon_db',
        sql="""

        INSERT INTO project_dwh.dim_time(date, days, month_id, month_name, year)
        SELECT 
            days.d::DATE, 
            to_char(days.d, 'FMMonth DD, YYYY'),
            to_char(days.d, 'MM')::integer,
            to_char(days.d, 'FMMonth'),
            to_char(days.d, 'YYYY')::integer
        FROM (
            SELECT generate_series(
                ('2000-01-01')::date,
                ('2100-12-31')::date,
                interval '1 day'
            )
        ) as days(d)
        WHERE NOT EXISTS (
            SELECT 1 FROM project_dwh.dim_time dt 
            WHERE dt.date = days.d
        );
        """
    )

    # DIM PRODUCT
    dim_product = SQLExecuteQueryOperator(
        task_id='dim_product',
        conn_id='neon_db',
        sql="""

        INSERT INTO project_dwh.dim_product (
            product_id, product_name, product_price, category_id, category_name, modify_datetime
        )
        SELECT 
            p.productid, 
            p.productname, 
            p.price, 
            p.categoryid, 
            c.categoryname,
            p.modifydate
        FROM project_stg.stg_products p
        LEFT JOIN project_stg.stg_categories c 
        ON p.categoryid = c.categoryid
        WHERE NOT EXISTS (
        SELECT 1 FROM project_dwh.dim_product dp
        WHERE dp.product_id = p.productid
        );
        """
    )

    # DIM CUSTOMER
    dim_customer = SQLExecuteQueryOperator(
        task_id='dim_customer',
        conn_id='neon_db',
        sql="""

        INSERT INTO project_dwh.dim_customer (
            customer_id, customer_first_name, customer_middle_initial_name,
            customer_last_name, customer_address, customer_city_id,
            customer_city_name, customer_zipcode,
            customer_country_id, customer_country_name, customer_country_code
        )
        SELECT 
            cust.customerid, 
            cust.firstname, 
            cust.middleinitial, 
            cust.lastname, 
            cust.address, 
            cust.cityid,
            ci.cityname, 
            ci.zipcode,
            ci.countryid, 
            co.countryname,
            co.countrycode
        FROM project_stg.stg_customers cust
        LEFT JOIN project_stg.stg_cities ci ON cust.cityid = ci.cityid
        LEFT JOIN project_stg.stg_countries co ON ci.countryid = co.countryid
        WHERE NOT EXISTS (
        SELECT 1 FROM project_dwh.dim_customer dc
        WHERE dc.customer_id = cust.customerid
        AND dc.end_date IS NULL
        );
        """
    )

    # DIM EMPLOYEE
    dim_employee = SQLExecuteQueryOperator(
        task_id='dim_employee',
        conn_id='neon_db',
        sql="""

        INSERT INTO project_dwh.dim_employee (
            employee_id, employee_first_name, employee_middle_initial,
            employee_last_name, employee_birth_date, employee_gender,
            employee_city_id, employee_city_name,
            employee_country_id, employee_country_name, employee_country_code,
            employee_hire_date, end_date
        )
        SELECT
            e.employeeid,
            e.firstname,
            e.middleinitial,
            e.lastname,
            e.birthdate,
            e.gender,
            e.cityid,
            c.cityname,
            co.countryid,
            co.countryname,
            co.countrycode,
            e.hiredate,
            NULL
        FROM project_stg.stg_employee e
        LEFT JOIN project_stg.stg_cities c ON e.cityid = c.cityid
        LEFT JOIN project_stg.stg_countries co ON c.countryid = co.countryid
        WHERE NOT EXISTS (
        SELECT 1 FROM project_dwh.dim_employee de
        WHERE de.employee_id = e.employeeid
        );
        """
    )

    # FACT SALES
    fact_sales = SQLExecuteQueryOperator(
        task_id='fact_sales',
        conn_id='neon_db',
        sql="""

        INSERT INTO project_dwh.fact_sales (
            sk_date, sk_customer, sk_employee, sk_product,
            sales_id, transaction_no, quantity, discount, total_price
        )
        SELECT 
            d.sk_date,
            c.sk_customer,
            e.sk_employee,
            p.sk_product,
            s.salesid,
            s.transactionnumber,
            s.quantity,
            s.discount,
            (s.quantity * p.product_price * (1 - s.discount)) AS total_price        
	FROM project_stg.stg_sales s
        LEFT JOIN project_dwh.dim_time d ON s.salesdate = d.date
        LEFT JOIN project_dwh.dim_customer c 
            ON s.customerid = c.customer_id AND c.end_date IS NULL
        LEFT JOIN project_dwh.dim_employee e 
            ON s.salespersonid = e.employee_id
        LEFT JOIN project_dwh.dim_product p 
            ON s.productid = p.product_id
        WHERE NOT EXISTS (
        SELECT 1 FROM project_dwh.fact_sales f
        WHERE f.sales_id = s.salesid
        );
        """
    )

    # FLOW
    load_task >> dim_time >> dim_product >> dim_customer >> dim_employee >> fact_sales