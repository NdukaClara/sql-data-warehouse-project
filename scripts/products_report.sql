---------------------------------REPORTING----------------------------
/* 
==========================================================
Product Report
==========================================================
Purpose:
    - This report consolidates key product metrics and behaviors

Highlighs: 
    1. Gathers essential fields such as product name, category, subcategory and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total qualtity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIS:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue
======================================================================
*/
--drop view gold.report_products

CREATE VIEW gold.report_products AS
with base_query as (
/* -------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
--------------------------------------------------------------  */
SELECT
    f.order_number,
    f.customer_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    p.product_key,
    p.product_number,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
)

, product_aggregations as (
    /* -------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------- */
select 
    product_key,
    product_number,
    product_name,
    category,
    subcategory,
    cost,
    count(distinct order_number) as total_orders,
    sum(sales_amount) as total_sales,
    sum(quantity) as total_quantity,
    count(distinct customer_key) as total_customers,
    max(order_date) as last_sale_date,
    DATEDIFF(month, min(order_date), max(order_date)) as lifespan,
    round(avg(cast(sales_amount as float) / nullif(quantity, 0)), 1) as avg_selling_price
from base_query
group BY
    product_key,
    product_number,
    product_name,
    category,
    subcategory,
    cost
)

select
    product_key,
    product_number,
    product_name,
    category,
    subcategory,
    cost,
    total_orders,
    total_sales,
    case when total_sales > 50000 then 'High-Performers'
         when total_sales >= 10000 then 'Mid-Range'
         else 'Low-Performers'
    end products_revenue,
    total_quantity,
    total_customers,
    last_sale_date,
    lifespan,
    DATEDIFF(month, last_sale_date, GETDATE()) as recency,
    avg_selling_price,
    -- compute avg order revenue (AOR)
    case when total_orders = 0 then 0
         else total_sales / total_orders
    end as avg_order_revenue,
    -- compute avg monthly revenue
    case when lifespan = 0 then 0
         else total_sales / lifespan
    end as avg_monthly_revenue
from product_aggregations

-- select * from gold.report_products
