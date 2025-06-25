/*
===============================================================================
-------------------ADVANCED ANALYTICS-----------------------------
===============================================================================
*/

-----------CHANGES OVER TIME ANALYSIS--------------
-- analyze how a measure evolves over time
-- helps track trends and identify seasonality in data

-- analyze sales performance over time
SELECT
year(order_date) as order_year,
DATETRUNC(month, order_date) as order_month,
sum(sales_amount) as total_sales,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date), DATETRUNC(month, order_date)
order by year(order_date)


-----------CUMULATIVE ANALYSIS--------------
-- aggregate data progressively over time
-- helps understand if the business is growing or declining

-- calc the total sales per month/year
-- and the running total of sales over time
SELECT
order_date,
total_sales,
sum(total_sales) OVER(order by order_date) as running_total_sales,
sum(average_price) OVER(order by order_date) as moving_average_price
FROM
(
SELECT
DATETRUNC(year, order_date) as order_date,
sum(sales_amount) as total_sales,
AVG(price) as average_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year, order_date)
)t


-----------PERFORMANCE ANALYSIS--------------
-- comapring the current value to a target value
-- helps measure success and compare performance

-- analyze the yearly performance of products by comparing their sales
-- to both the avg sales performance of the product and the previous year's sales 

with yearly_product_sales AS (
select 
    year(f.order_date) as order_year,
    p.product_name as product_name,
    sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on        f.product_key = p.product_key
where order_date is not null
group by 
    year(f.order_date),
    p.product_name
)

select 
order_year,
product_name,
current_sales,
AVG(current_sales) over(PARTITION by product_name) as avg_sales,
current_sales - AVG(current_sales) over(PARTITION by product_name) as diff_avg,
case when current_sales - AVG(current_sales) over(PARTITION by product_name) > 0 then 'Above Avg'
     when current_sales - AVG(current_sales) over(PARTITION by product_name) < 0 then 'Below Avg'
     else 'Avg'
end avg_change,
-- Year-Over-Year Analysis
lag(current_sales) OVER(PARTITION by product_name order by order_year) as prev_sales,
current_sales - lag(current_sales) OVER(PARTITION by product_name order by order_year) as diff_prev,
case when current_sales - lag(current_sales) OVER(PARTITION by product_name order by order_year) > 0 then 'Increase'
     when current_sales - lag(current_sales) OVER(PARTITION by product_name order by order_year) < 0 then 'Decrease'
     else 'No Change'
end prev_change
from yearly_product_sales


-----------PART-TO-WHOLE (PROPORTIONAL) ANALYSIS--------------
-- analyze how an individual part is performing compared to the overall,
-- allowing us to understand which category has the greatest impact on the business

-- which categories contribute the most to the overall sales?

with category_sales as (
SELECT
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key 
group by category
)

select
category,
total_sales,
sum(total_sales) OVER() overall_sales,
concat(round((cast(total_sales as float) / sum(total_sales) OVER()) * 100, 2), '%') as percentage_total
from category_sales
order by total_sales desc


-----------DATA SEGMENTATION--------------
-- group data based on a specific range
-- helps to understand the correlation btw two measures


-- segment products into cost range and 
-- count how many products fall into each segment
with products_segment as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'above 1000'
end cost_range
from gold.dim_products 
)

select
cost_range,
count(product_key) as total_products
from products_segment
group by cost_range
order by total_products desc

/* 
Group customers into three segments based on their spending behavior:
- VIP: at least 12 months of history and spending above £5000
- Regular: at least 12 months of history but spending £5000 or less
- New: lifespan less than 12 months 
And find the total number of customers by each group
*/
with customer_spending as (
SELECT
    c.customer_key,
    sum(f.sales_amount) as total_spending,
    min(f.order_date) as first_order,
    max(f.order_date) as last_order,
    DATEDIFF(month, min(f.order_date), max(f.order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customer c
on f.customer_key = c.customer_key
group by c.customer_key
)

select 
    customer_segment,
    count(customer_key) as total_customers
from (
    SELECT
    customer_key,
    total_spending,
    lifespan,
    case when lifespan >= 12 and total_spending > 5000 then 'VIP'
        when lifespan >= 12 and total_spending <= 5000 then 'Regular'
        else 'New'
    end customer_segment
    from customer_spending
)t group by customer_segment
order by total_customers desc
