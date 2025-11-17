CREATE SCHEMA IF NOT EXISTS datamart;
CREATE SCHEMA IF NOT EXISTS staging;
-- control
CREATE TABLE IF NOT EXISTS datamart.control_loads (
  process_name text PRIMARY KEY,
  last_run timestamptz,
  rows_extracted bigint,
  rows_loaded bigint,
  last_status text,
  last_message text
);

-- dim_date
CREATE TABLE IF NOT EXISTS datamart.dim_date (
  date_key integer PRIMARY KEY, -- YYYYMMDD
  date_actual date NOT NULL,
  year smallint, quarter smallint, month smallint, day smallint, weekday smallint
);

-- dim_customer (SCD2)
CREATE TABLE IF NOT EXISTS datamart.dim_customer (
  customer_sk bigserial PRIMARY KEY,
  customer_pk bigint NOT NULL,
  customer_name text,
  email text,
  city text,
  state text,
  country text,
  start_date timestamptz,
  end_date timestamptz,
  is_current boolean DEFAULT true
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_pk_current
  ON datamart.dim_customer(customer_pk) WHERE is_current;

-- dim_product
CREATE TABLE IF NOT EXISTS datamart.dim_product (
  product_sk bigserial PRIMARY KEY,
  product_pk bigint UNIQUE,
  product_name text,
  product_category text,
  product_subcategory text,
  list_price numeric(18,4)
);

-- dim_reseller
CREATE TABLE IF NOT EXISTS datamart.dim_reseller (
  reseller_sk bigserial PRIMARY KEY,
  reseller_pk bigint UNIQUE,
  reseller_name text,
  country text
);

-- facts
CREATE TABLE IF NOT EXISTS datamart.fact_internet_sales (
  sale_sk bigserial PRIMARY KEY,
  sale_pk bigint,
  date_key integer REFERENCES datamart.dim_date(date_key),
  customer_sk bigint REFERENCES datamart.dim_customer(customer_sk),
  product_sk bigint REFERENCES datamart.dim_product(product_sk),
  quantity int,
  unit_price numeric(18,4),
  subtotal numeric(18,4),
  tax numeric(18,4),
  freight numeric(18,4),
  total numeric(18,4),
  load_ts timestamptz default now()
);

CREATE TABLE IF NOT EXISTS datamart.fact_reseller_sales (
  sale_sk bigserial PRIMARY KEY,
  sale_pk bigint,
  date_key integer REFERENCES datamart.dim_date(date_key),
  reseller_sk bigint REFERENCES datamart.dim_reseller(reseller_sk),
  product_sk bigint REFERENCES datamart.dim_product(product_sk),
  quantity int,
  unit_price numeric(18,4),
  subtotal numeric(18,4),
  tax numeric(18,4),
  freight numeric(18,4),
  total numeric(18,4),
  load_ts timestamptz default now()
);

CREATE TABLE IF NOT EXISTS datamart.dim_salesperson (
    salesperson_pk bigint PRIMARY KEY,
    salesperson_name text
);