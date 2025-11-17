CREATE TABLE IF NOT EXISTS staging.stg_sales_orders (
  order_id bigint,
  order_date timestamptz,
  online_order boolean,
  customer_id bigint,
  store_id bigint,
  sales_person_id bigint,
  billtoaddressid int,
  shiptoaddressid int,
  subtotal numeric(18,4),
  taxamt numeric(18,4),
  freight numeric(18,4),
  totaldue numeric(18,4),
  created_at timestamptz default now()
);

CREATE TABLE IF NOT EXISTS staging.stg_sales_order_items (
  order_id bigint,
  order_detail_id bigint,
  product_id bigint,
  unit_price numeric(18,4),
  order_qty int,
  line_total numeric(18,4)
);
