#!/usr/bin/env python3
"""
ETL script: AdventureWorks -> datamart (Postgres)
Genera data mart:
 - fact_internet_sales
 - fact_reseller_sales

Flujo:
 - leer last_run
 - extraer headers, items, customers, products, resellers, salespersons desde SQL Server
 - volcar CSVs temporales y COPY a staging schema en Postgres
 - upsert dims (product, reseller, salesperson) (Type 1)
 - SCD Type 2 para dim_customer
 - poblar dim_date
 - mapear surrogate keys y cargar hechos
 - actualizar control_loads
"""

import os
import sys
import logging
import tempfile
from datetime import datetime, timezone
import pandas as pd
import pyodbc
import psycopg2
import psycopg2.extras as pg_extras

# ---------------------------
# CONFIG (ajusta según tu entorno)
# ---------------------------
SQLSERVER_CONN = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-MOFS9R9;DATABASE=AdventureWorks2022;UID=junior_user;PWD=miss"
PG_CONN_DSN = "host=localhost dbname=adventureWorksDatamart user=postgres password=mipg"

PROCESS_NAME = "internet_reseller_etl"

# ---------------------------
# EXTRACTION QUERIES (SQL Server)
# ---------------------------
SQL_HEADERS = """
SELECT soh.SalesOrderID AS order_id,
       soh.OrderDate AS order_date,
       soh.OnlineOrderFlag AS online_order,
       soh.CustomerID AS customer_id,
       c.StoreID AS store_id,
       soh.SalesPersonID AS sales_person_id,
       soh.BillToAddressID AS billtoaddressid,
       soh.ShipToAddressID AS shiptoaddressid,
       soh.SubTotal AS subtotal,
       soh.TaxAmt AS taxamt,
       soh.Freight AS freight,
       soh.TotalDue AS totaldue,
       soh.ModifiedDate AS created_at
FROM Sales.SalesOrderHeader soh
LEFT JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
WHERE soh.OrderDate >= ?
ORDER BY soh.OrderDate;
"""

SQL_ITEMS = """
SELECT sod.SalesOrderID as order_id,
       sod.SalesOrderDetailID as order_detail_id,
       sod.ProductID as product_id,
       sod.UnitPrice as unit_price,
       sod.OrderQty as order_qty,
       sod.LineTotal as line_total
FROM Sales.SalesOrderDetail sod
JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
WHERE soh.OrderDate >= ?
ORDER BY soh.OrderDate;
"""

# Extract customers with simple attributes (firstname, lastname, email, city, state, country)
SQL_CUSTOMERS = """
SELECT DISTINCT c.CustomerID AS customer_id,
       p.FirstName AS first_name,
       p.LastName AS last_name,
       ea.EmailAddress AS email,
       addr.City AS city,
       sp.Name AS state,
       cr.Name AS country
FROM Sales.Customer c
LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
LEFT JOIN Person.EmailAddress ea ON ea.BusinessEntityID = p.BusinessEntityID
LEFT JOIN Person.BusinessEntityAddress bea ON bea.BusinessEntityID = p.BusinessEntityID
LEFT JOIN Person.Address addr ON bea.AddressID = addr.AddressID
LEFT JOIN Person.StateProvince sp ON addr.StateProvinceID = sp.StateProvinceID
LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
WHERE c.CustomerID IN (
  SELECT DISTINCT CustomerID FROM Sales.SalesOrderHeader WHERE OrderDate >= ?
)
"""

# Extract products referenced in items (fast way)
SQL_PRODUCTS = """
SELECT p.ProductID as product_id, p.Name as product_name, pc.Name as category, p.ListPrice as list_price
FROM Production.Product p
LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
WHERE p.ProductID IN (
  SELECT DISTINCT sod.ProductID
  FROM Sales.SalesOrderDetail sod
  JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
  WHERE soh.OrderDate >= ?
)
"""

# Extract resellers (stores)
SQL_RESELLERS = """
SELECT DISTINCT
       c.StoreID AS store_id,
       s.Name    AS store_name,
       st.Name   AS territory
FROM Sales.SalesOrderHeader soh
LEFT JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
LEFT JOIN Sales.SalesTerritory st ON c.TerritoryID = st.TerritoryID
WHERE soh.OrderDate >= ?
"""

# Extract salespersons referenced
SQL_SALESPERSONS = """
SELECT sp.BusinessEntityID as salesperson_id, pe.FirstName, pe.LastName
FROM Sales.SalesPerson sp
LEFT JOIN HumanResources.Employee he ON sp.BusinessEntityID = he.BusinessEntityID
LEFT JOIN Person.Person pe ON he.BusinessEntityID = pe.BusinessEntityID
WHERE sp.BusinessEntityID IN (
  SELECT DISTINCT SalesPersonID FROM Sales.SalesOrderHeader WHERE OrderDate >= ?
)
"""

TMP_DIR = os.path.join(os.getcwd(), "tmp")
os.makedirs(TMP_DIR, exist_ok=True)

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("etl")

# ---------------------------
# HELPERS DB
# ---------------------------
def get_pg_conn():
    return psycopg2.connect(PG_CONN_DSN)

def get_sqlserver_conn():
    return pyodbc.connect(SQLSERVER_CONN)

# ---------------------------
# CONTROL: last_run
# ---------------------------
def get_last_run(pg_conn, process_name=PROCESS_NAME):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT last_run FROM datamart.control_loads WHERE process_name = %s
        """, (process_name,))
        r = cur.fetchone()
        if r and r[0]:
            return r[0]
        else:
            # default far past date for initial full load
            return datetime(2000,1,1, tzinfo=timezone.utc)

def upsert_control(pg_conn, process_name, last_run, rows_extracted, rows_loaded, status, message):
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO datamart.control_loads (process_name, last_run, rows_extracted, rows_loaded, last_status, last_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (process_name) DO UPDATE
            SET last_run = EXCLUDED.last_run,
                rows_extracted = EXCLUDED.rows_extracted,
                rows_loaded = EXCLUDED.rows_loaded,
                last_status = EXCLUDED.last_status,
                last_message = EXCLUDED.last_message;
        """, (process_name, last_run, rows_extracted, rows_loaded, status, message))
    pg_conn.commit()

# ---------------------------
# EXTRACT -> CSV & COPY to staging
# ---------------------------
def extract_to_csv_sqlserver(sql, param_date, out_csv_path, int_cols=None, bool_cols=None, date_cols=None, float_cols=None):
    r"""
    Extrae con pandas desde SQL Server y normaliza columnas antes de escribir CSV:
     - int_cols: lista de nombres de columnas que deben ser enteras (bigint). Convertimos a texto sin .0 y NULL -> '\N'
     - bool_cols: lista de boolean columns; normalizamos a 't'/'f' (Postgres boolean)
     - date_cols: columnas fecha que queremos formatear a 'YYYY-MM-DD HH:MM:SS+00' o 'YYYY-MM-DD HH:MM:SS'
     - float_cols: columnas numéricas que no queremos perder decimales (se mantienen como strings)
    Devuelve (num_rows, path_csv)
    """
    log.info("Connecting to SQL Server to extract: %s", sql.splitlines()[0].strip())
    with get_sqlserver_conn() as src_conn:
        df = pd.read_sql(sql, src_conn, params=[param_date])

    # If empty, write an empty CSV with headers and return
    if df.empty:
        df.to_csv(out_csv_path, index=False, encoding='utf-8', na_rep='\\N')
        log.info("Wrote 0 rows to %s (empty result)", out_csv_path)
        return 0, out_csv_path

    # Normalize integer columns (to string w/o decimals; NULL -> '\N')
    if int_cols:
        def conv_int_no_null(v):
            if pd.isna(v) or v in ('', r'\N', None):
                return '0'
            try:
                if isinstance(v, str) and v.endswith('.0'):
                    v = v[:-2]
                return str(int(float(v)))
            except Exception:
                return '0'
        for c in int_cols:
            if c in df.columns:
                df[c] = df[c].apply(conv_int_no_null)
            else:
                log.debug("Int column %s not present in df", c)

    # Normalize boolean columns to 't'/'f' or '\N'
    if bool_cols:
        for c in bool_cols:
            if c in df.columns:
                def conv_bool(v):
                    if pd.isna(v):
                        return r'\N'
                    try:
                        # some sources use 0/1 or True/False
                        if isinstance(v, (int, float)):
                            return 't' if int(v) != 0 else 'f'
                        if isinstance(v, str):
                            vl = v.strip().lower()
                            if vl in ('1','t','true','yes','y'):
                                return 't'
                            if vl in ('0','f','false','no','n'):
                                return 'f'
                            return r'\N'
                        return 't' if bool(v) else 'f'
                    except Exception:
                        return r'\N'
                df[c] = df[c].apply(conv_bool)
            else:
                log.debug("Bool column %s not present in df", c)

    # Normalize date/time columns to 'YYYY-MM-DD HH:MM:SS' (no tz) or ISO
    if date_cols:
        for c in date_cols:
            if c in df.columns:
                def conv_date(v):
                    if pd.isna(v):
                        return r'\N'
                    try:
                        # try to parse with pandas (handles many formats incl. Excel serial if read as float)
                        ts = pd.to_datetime(v, errors='coerce')
                        if pd.isna(ts):
                            # if couldn't parse, try interpreting Excel serial numbers (days since 1899-12-30)
                            try:
                                f = float(v)
                                # Excel serial -> pandas Timestamp
                                excel_epoch = pd.Timestamp('1899-12-30')
                                ts2 = excel_epoch + pd.to_timedelta(int(f), unit='d') + pd.to_timedelta((f - int(f)) * 86400, unit='s')
                                return ts2.strftime('%Y-%m-%d %H:%M:%S')
                            except Exception:
                                return r'\N'
                        # Format without timezone for Postgres timestamptz acceptance; Postgres can parse 'YYYY-MM-DD HH:MM:SS'
                        return ts.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        return r'\N'
                df[c] = df[c].apply(conv_date)
            else:
                log.debug("Date column %s not present in df", c)

    # Normalize float columns (if provided) to string with dot decimal, NULL -> '\N'
    if float_cols:
        for c in float_cols:
            if c in df.columns:
                def conv_float(v):
                    if pd.isna(v):
                        return r'\N'
                    try:
                        return str(float(v))
                    except Exception:
                        return r'\N'
                df[c] = df[c].apply(conv_float)
            else:
                log.debug("Float column %s not present in df", c)

    # For any remaining NaN (other columns), replace with '\N'
    df = df.fillna(r'\N')

    # IMPORTANT: ensure all values are strings (so pandas won't emit floats with .0 by default)
    for col in df.columns:
        if df[col].dtype != object:
            df[col] = df[col].astype(str)

    # write CSV
    df.to_csv(out_csv_path, index=False, encoding='utf-8', na_rep='\\N')
    log.info("Wrote %d rows to %s", len(df), out_csv_path)
    return len(df), out_csv_path

def copy_csv_to_staging(pg_conn, csv_path, table):
    r"""
    COPY con especificación de NULL como '\N' para que Postgres interprete '\N' como NULL.
    """
    log.info("COPYing %s -> staging.%s", csv_path, table)
    with open(csv_path, "r", encoding="utf-8") as f, pg_conn.cursor() as cur:
        # in the Python source string we escape the backslash twice so SQL receives NULL '\N'
        copy_sql = f"COPY staging.{table} FROM STDIN WITH CSV HEADER DELIMITER ',' NULL '\\\\N';"
        cur.copy_expert(copy_sql, f)
    pg_conn.commit()

# ---------------------------
# UPSERT DIMENSIONS
# ---------------------------
def upsert_dim_products(pg_conn, products_csv):
    df = pd.read_csv(products_csv)
    records = [(int(row.product_id), row.product_name, row.category if not pd.isna(row.category) else None, float(row.list_price) if not pd.isna(row.list_price) else None) for row in df.itertuples(index=False)]
    if not records:
        return 0
    sql = """
    INSERT INTO datamart.dim_product (product_pk, product_name, product_category, list_price)
    VALUES %s
    ON CONFLICT (product_pk) DO UPDATE
      SET product_name = EXCLUDED.product_name,
          product_category = EXCLUDED.product_category,
          list_price = EXCLUDED.list_price
    """
    with pg_conn.cursor() as cur:
        pg_extras.execute_values(cur, sql, records, page_size=1000)
    pg_conn.commit()
    log.info("Upserted %d products", len(records))
    return len(records)

def upsert_dim_resellers(pg_conn, resellers_csv):
    df = pd.read_csv(resellers_csv)
    df = df.drop_duplicates(subset=['store_id'])
    recs = []
    for r in df.itertuples(index=False):
        recs.append((int(r.store_id), r.store_name, r.territory if not pd.isna(r.territory) else None))
    if not recs:
        return 0
    sql = """
    INSERT INTO datamart.dim_reseller (reseller_pk, reseller_name, country)
    VALUES %s
    ON CONFLICT (reseller_pk) DO UPDATE
      SET reseller_name = EXCLUDED.reseller_name,
          country = EXCLUDED.country
    """
    with pg_conn.cursor() as cur:
        pg_extras.execute_values(cur, sql, recs, page_size=500)
    pg_conn.commit()
    log.info("Upserted %d resellers", len(recs))
    return len(recs)

def upsert_dim_salespersons(pg_conn, salespersons_csv):
    df = pd.read_csv(salespersons_csv)
    recs = []
    for r in df.itertuples(index=False):
        name = f"{r.FirstName or ''} {r.LastName or ''}".strip()
        recs.append((int(r.salesperson_id), name))
    if not recs:
        return 0
    sql = """
    INSERT INTO datamart.dim_salesperson (salesperson_pk, salesperson_name)
    VALUES %s
    ON CONFLICT (salesperson_pk) DO UPDATE
      SET salesperson_name = EXCLUDED.salesperson_name
    """
    with pg_conn.cursor() as cur:
        pg_extras.execute_values(cur, sql, recs, page_size=500)
    pg_conn.commit()
    log.info("Upserted %d salespersons", len(recs))
    return len(recs)

# ---------------------------
# SCD Type 2 for customers
# ---------------------------
def scd2_customers(pg_conn, customers_csv):
    """
    Implementa SCD Type 2 para dim_customer:
    - compara staging distinct customers con la fila current (is_current = true)
    - si no existe, inserta
    - si existe y atributos cambiaron, cierra la antigua (end_date = now(), is_current=false) y crea nueva fila current
    """
    df = pd.read_csv(customers_csv)
    if df.empty:
        return 0

    # Normalize columns and make customer_name
    df['customer_pk'] = df['customer_id'].astype(int)
    df['customer_name'] = (df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')).str.strip()
    df_to_use = df[['customer_pk', 'customer_name', 'email', 'city', 'state', 'country']].drop_duplicates(subset=['customer_pk'])

    # load into a temporary table in Postgres
    tmp_table = "tmp_customers"
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        cur.execute(f"""
            CREATE TEMPORARY TABLE {tmp_table} (
            customer_pk bigint PRIMARY KEY,
            customer_name text,
            email text,
            city text,
            state text,
            country text
            ) ON COMMIT DROP
        """)
        # bulk insert using execute_values
        rows = [tuple(x) for x in df_to_use[['customer_pk','customer_name','email','city','state','country']].fillna('').values]
        if not rows:
            log.info("No customers to process")
            return 0
        insert_sql = f"INSERT INTO {tmp_table} (customer_pk, customer_name, email, city, state, country) VALUES %s"
        pg_extras.execute_values(cur, insert_sql, rows, page_size=500)

        # 1) close existing current records where attributes differ
        cur.execute("""
        UPDATE datamart.dim_customer dc
        SET end_date = now(), is_current = false
        FROM tmp_customers t
        WHERE dc.customer_pk = t.customer_pk
          AND dc.is_current = true
          AND (
            coalesce(dc.customer_name,'') <> coalesce(t.customer_name,'') OR
            coalesce(dc.email,'') <> coalesce(t.email,'') OR
            coalesce(dc.city,'') <> coalesce(t.city,'') OR
            coalesce(dc.state,'') <> coalesce(t.state,'') OR
            coalesce(dc.country,'') <> coalesce(t.country,'')
          )
        """)
        updated = cur.rowcount

        # 2) insert new rows for customers that either: no current row or were changed (we use LEFT JOIN)
        cur.execute("""
        INSERT INTO datamart.dim_customer (customer_pk, customer_name, email, city, state, country, start_date, is_current)
        SELECT t.customer_pk, t.customer_name, nullif(t.email,''), nullif(t.city,''), nullif(t.state,''), nullif(t.country,''), now(), true
        FROM tmp_customers t
        LEFT JOIN datamart.dim_customer dc ON dc.customer_pk = t.customer_pk AND dc.is_current = true
        WHERE dc.customer_pk IS NULL
           OR (coalesce(dc.customer_name,'') <> coalesce(t.customer_name,'')
               OR coalesce(dc.email,'') <> coalesce(t.email,'')
               OR coalesce(dc.city,'') <> coalesce(t.city,'')
               OR coalesce(dc.state,'') <> coalesce(t.state,'')
               OR coalesce(dc.country,'') <> coalesce(t.country,''))
        """)
        inserted = cur.rowcount

    pg_conn.commit()
    log.info("SCD2 customers: closed %d current rows, inserted %d new current rows", updated, inserted)
    return updated + inserted

# ---------------------------
# POPULAR dim_date
# ---------------------------
def populate_dim_date(pg_conn):
    # insert distinct date_keys from staging.stg_sales_orders
    with pg_conn.cursor() as cur:
        cur.execute("""
        INSERT INTO datamart.dim_date (date_key, date_actual, year, quarter, month, day, weekday)
        SELECT DISTINCT (to_char(date_trunc('day', order_date), 'YYYYMMDD'))::int as date_key,
               date_trunc('day', order_date)::date as date_actual,
               EXTRACT(YEAR FROM order_date)::int,
               EXTRACT(QUARTER FROM order_date)::int,
               EXTRACT(MONTH FROM order_date)::int,
               EXTRACT(DAY FROM order_date)::int,
               EXTRACT(ISODOW FROM order_date)::int
        FROM staging.stg_sales_orders
        WHERE order_date IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING
        """)
    pg_conn.commit()
    log.info("Populated dim_date from staging")

# ---------------------------
# LOAD facts
# ---------------------------
def load_facts(pg_conn):
    # Internet sales
    with pg_conn.cursor() as cur:
        cur.execute("""
        INSERT INTO datamart.fact_internet_sales (sale_pk, date_key, customer_sk, product_sk, quantity, unit_price, subtotal, tax, freight, total)
        SELECT s.order_id,
            (to_char(s.order_date,'YYYYMMDD'))::int as date_key,
            dc.customer_sk,
            dp.product_sk,
            i.order_qty,
            i.unit_price,
            s.subtotal,
            s.taxamt,
            s.freight,
            s.totaldue
        FROM staging.stg_sales_orders s
        JOIN staging.stg_sales_order_items i ON s.order_id = i.order_id
        LEFT JOIN datamart.dim_product dp ON dp.product_pk = i.product_id
        LEFT JOIN datamart.dim_customer dc ON dc.customer_pk = s.customer_id AND dc.is_current = true
        WHERE s.online_order IS TRUE;
        """)
        internet_rows = cur.rowcount

        # Reseller sales
        cur.execute("""
        INSERT INTO datamart.fact_reseller_sales (sale_pk, date_key, reseller_sk, product_sk, quantity, unit_price, subtotal, tax, freight, total)
        SELECT s.order_id,
               (to_char(s.order_date,'YYYYMMDD'))::int as date_key,
               dr.reseller_sk,
               dp.product_sk,
               i.order_qty,
               i.unit_price,
               s.subtotal,
               s.taxamt,
               s.freight,
               s.totaldue
        FROM staging.stg_sales_orders s
        JOIN staging.stg_sales_order_items i ON s.order_id = i.order_id
        LEFT JOIN datamart.dim_product dp ON dp.product_pk = i.product_id
        LEFT JOIN datamart.dim_reseller dr ON dr.reseller_pk = s.store_id
        WHERE s.store_id IS NOT NULL
        """)
        reseller_rows = cur.rowcount

    pg_conn.commit()
    log.info("Inserted %s internet fact rows and %s reseller fact rows", internet_rows, reseller_rows)
    return internet_rows + reseller_rows

# ---------------------------
# MAIN FLOW
# ---------------------------
def main():
    log.info("ETL process started")
    pg_conn = get_pg_conn()

    try:
        last_run = get_last_run(pg_conn)
        log.info("Last run reported: %s", last_run)

        # For initial load set a cutoff date far back (or leave last_run)
        cutoff = datetime(2010, 1, 1, tzinfo=timezone.utc)

        # 1) Extract headers and items
        tmp_headers = os.path.join(TMP_DIR, "stg_headers.csv")
        tmp_items = os.path.join(TMP_DIR, "stg_items.csv")
        tmp_customers = os.path.join(TMP_DIR, "stg_customers.csv")
        tmp_products = os.path.join(TMP_DIR, "stg_products.csv")
        tmp_resellers = os.path.join(TMP_DIR, "stg_resellers.csv")
        tmp_salespersons = os.path.join(TMP_DIR, "stg_salespersons.csv")

        rows_hdr, _ = extract_to_csv_sqlserver(
                    SQL_HEADERS, cutoff, tmp_headers,
                    int_cols=['order_id','customer_id','store_id','sales_person_id','billtoaddressid','shiptoaddressid'],
                    bool_cols=['online_order'],
                    date_cols=['order_date','created_at'],   # <-- asegúrate que 'created_at' es el nombre exacto
                    float_cols=['subtotal','taxamt','freight','totaldue'],

        )

        rows_itm, _ = extract_to_csv_sqlserver(SQL_ITEMS, cutoff, tmp_items,
                                            int_cols=['order_id','order_detail_id','product_id'])
        rows_cust, _ = extract_to_csv_sqlserver(SQL_CUSTOMERS, cutoff, tmp_customers,
                                                int_cols=['customer_id'])
        rows_prod, _ = extract_to_csv_sqlserver(SQL_PRODUCTS, cutoff, tmp_products,
                                                int_cols=['product_id'])
        rows_res, _ = extract_to_csv_sqlserver(SQL_RESELLERS, cutoff, tmp_resellers,
                                            int_cols=['store_id'])
        rows_sp, _ = extract_to_csv_sqlserver(SQL_SALESPERSONS, cutoff, tmp_salespersons,
                                            int_cols=['salesperson_id'])
        total_extracted = rows_hdr + rows_itm + rows_cust + rows_prod + rows_res + rows_sp
        log.info("Total rows extracted (sum of csvs): %d", total_extracted)

        # 2) COPY CSVs to staging
        copy_csv_to_staging(pg_conn, tmp_headers, "stg_sales_orders")
        copy_csv_to_staging(pg_conn, tmp_items, "stg_sales_order_items")
        # customers/products/resellers/salespersons will be used for dims upserts
        # we don't copy customers to staging tables; we use the CSV directly for SCD2
        copy_csv_to_staging(pg_conn, tmp_products, "stg_products_temp") if False else None  # optional

        # 3) Upsert dims (products, resellers, salespersons)
        upsert_dim_products(pg_conn, tmp_products)
        upsert_dim_resellers(pg_conn, tmp_resellers)
        upsert_dim_salespersons(pg_conn, tmp_salespersons)

        # 4) SCD2 customers
        scd_count = scd2_customers(pg_conn, tmp_customers)

        # 5) Populate dim_date
        populate_dim_date(pg_conn)

        # 6) Load facts
        rows_loaded = load_facts(pg_conn)

        # 7) update control table with now() as last_run
        now_ts = datetime.now(timezone.utc)
        upsert_control(pg_conn, PROCESS_NAME, now_ts, total_extracted, rows_loaded, "OK", f"Loaded {rows_loaded} rows")
        log.info("ETL completed OK. Rows loaded: %d", rows_loaded)

    except Exception as e:
        log.exception("ETL failed: %s", str(e))
        # update control table with failure
        try:
            upsert_control(pg_conn, PROCESS_NAME, datetime.now(timezone.utc), 0, 0, "ERROR", str(e))
        except Exception:
            pass
        raise
    finally:
        try:
            pg_conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()