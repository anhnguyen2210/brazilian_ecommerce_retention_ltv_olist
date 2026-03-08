
-- REVIEW DATA TABLE
select *
from read_csv_auto('/content/drive/MyDrive/Data Project - Brazillian/olist_orders_dataset.csv')
;



-- CHECK ORDER STATUS DISTRIBUTION 
select order_status, count(*)
from read_csv_auto('/content/drive/MyDrive/Data Project - Brazillian/olist_orders_dataset.csv')
group by 1 
;




-- CREATE TABLE FOR DATA ANALYSIS 
create or replace table orders_clean as
  with a as (
    select
      o.order_id,
      c.customer_unique_id,
      o.order_purchase_timestamp,
      o.order_status,
      p.payment_value as revenue
    from read_csv_auto('/content/drive/MyDrive/Data Project - Brazillian/olist_orders_dataset.csv') o
      left join read_csv_auto('/content/drive/MyDrive/Data Project - Brazillian/olist_customers_dataset.csv') c
on o.customer_id = c.customer_id
      left join read_csv_auto('/content/drive/MyDrive/Data Project - Brazillian/olist_order_payments_dataset.csv') p
              on o.order_id = p.order_id
    where o.order_status = 'delivered'
    )
    select
      a.order_id,
      a.customer_unique_id,
      cast(a.order_purchase_timestamp as timestamp) as purchase_ts,
      revenue
    from a
;




-- FIRT PURCHASE OF EACH CUSTOMER
  create or replace table customer_first_purchase as
  select
      customer_unique_id,
      min(purchase_ts) as first_purchase_ts
  from orders_clean
  group by customer_unique_id
  order by 2
;




-- COHORT BASE 
create or replace table cohort_base as
with
  cohort_table as (
    select
        customer_unique_id,
        first_purchase_ts,
        date_trunc('month', first_purchase_ts) as cohort_month
    from customer_first_purchase
  )
  select
    o.customer_unique_id,
    o.purchase_ts,
    c.first_purchase_ts,
    c.cohort_month,
    date_diff('month', c.first_purchase_ts, o.purchase_ts) as month_number,
    o.revenue
  from orders_clean o join cohort_table c
       on o.customer_unique_id = c.customer_unique_id
;




-- ACTIVE USERS
  select
      cohort_month,
      month_number,
      count(distinct customer_unique_id) as active_users
  from cohort_base
  group by 1,2
  order by 1,2
;




-- ORDER BY CUSTOMER
  select
      customer_unique_id,
      count(*) as order_count
  from orders_clean
  group by customer_unique_id
;




-- # LTV Analysis
select
  customer_unique_id,
  sum(revenue) as total_revenue,
  count(*) as order_count
from orders_clean
group by customer_unique_id

