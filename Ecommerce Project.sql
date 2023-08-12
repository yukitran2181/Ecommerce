-- Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
With parsed_table as (
SELECT format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
      ,visitId
      ,totals.pageviews
      ,totals.transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
Where _table_suffix between '0101'and '0331')

SELECT month
      , count(visitId) as visits
      , sum(pageviews) as pageviews
      , sum(transactions) as transactions
FROM parsed_table
Group by parsed_table.month
ORDER by parsed_table.month ASC;


-- Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
-- Output: source, total_visits, total_no_of bounce, bounce_rate
-- Problem:
-- 1.july 2017
-- 2.total_visit
-- 3. num_bounce

SELECT *,
      100*total_no_of_bounces/total_visits as bounce_rate
FROM
      (SELECT trafficSource.source
      ,count(visitId) as total_visits
      ,sum(totals.bounces) as total_no_of_bounces
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
      GROUP BY trafficSource.source
      ORDER BY trafficSource.source)
ORDER BY total_visits DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017
-- Get all the data from 06/2017
-- Fix the data type of revenue
With month_revenue as (
            SELECT 'Month' as time_type
                  ,format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) as time
                  ,trafficSource.source
                  ,sum(productRevenue)/POW(10,6) as revenue 
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
            , UNNEST(hits) AS hits
                  , UNNEST(hits.product)
            Group by time_type, time, trafficSource.source),

week_revenue as(
            SELECT 'Week' as time_type
                  ,Concat ('2017',EXTRACT(ISOWEEK FROM PARSE_DATE("%Y%m%d",date))) as time
                  ,trafficSource.source
                  ,sum(productRevenue)/POW(10,6) as revenue
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
            , UNNEST(hits) AS hits
                  , UNNEST(hits.product)
            Where productRevenue is not null
            Group by time_type, time, trafficSource.source)

SELECT * from week_revenue
UNION ALL
SELECT * from month_revenue
Order by revenue DESC;


-- Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) 
-- in June, July 2017.

-- Total_page_view_purchasers
With Total_page_view_purchasers as 
(SELECT month,
      sum(views)/count(distinct fullVisitorId ) as avg_pageviews_purchase
FROM 
      (SELECT format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
            fullVisitorId,
            totals.totalTransactionRevenue,
            sum(productRevenue) as revenue,
            sum(totals.pageviews) as views
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      , UNNEST(hits) AS hits
      , UNNEST(hits.product)
      Where (_table_suffix between '0601'and '0731'
      and productRevenue is not null
      AND totals.totalTransactionRevenue is not null)
      Group by month,  fullVisitorId, totals.totalTransactionRevenue
      Order by month,  fullVisitorId, totals.totalTransactionRevenue)
Group by month),

-- Total_page_view_non_purchasers
Total_page_view_non_purchasers as(SELECT month,
      sum(views)/count(distinct fullVisitorId ) as avg_pageviews_non_purchase
FROM
      (SELECT format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
            fullVisitorId,
            totals.totalTransactionRevenue,
            sum(productRevenue) as revenue,
            sum(totals.pageviews) as views
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      , UNNEST(hits) AS hits
      , UNNEST(hits.product)
      Where (_table_suffix between '0601'and '0731'
      And productRevenue is null
      AND totals.totalTransactionRevenue is null)
      Group by month, fullVisitorId,totals.totalTransactionRevenue
      Order by month, fullVisitorId,totals.totalTransactionRevenue)
Group by month)

-- Tổng hợp tất cả kết quả bằng cách join 
SELECT *
FROM Total_page_view_purchasers
LEFT JOIN Total_page_view_non_purchasers
using (month)
Order by month;

-- It can be rewrited as follows
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  --and totals.totalTransactionRevenue is not null
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;



-- Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
      -- fullVisitorId,
      -- totals.transactions,
      -- productRevenue
      (sum(totals.transactions)/
      count(distinct(fullVisitorId))) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      , UNNEST(hits) AS hits
      , UNNEST(hits.product)
Where (totals.transactions >=1
And productRevenue is not null)
Group by month;


-- Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
SELECT format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
      -- fullVisitorId,
      -- totals.transactions,
      -- productRevenue
      (sum(productRevenue/POW(10,6))/ sum(totals.visits)) as avg_spendpersession
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      , UNNEST(hits) AS hits
      , UNNEST(hits.product)
Where (totals.transactions >=1
And productRevenue is not null)
Group by month;


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
-- List all customers who purchased "YouTube Men's Vintage Henley"

SELECT 
      distinct(v2ProductName) as other_purchased_products,
      sum(productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      , UNNEST(hits) AS hits
      , UNNEST(hits.product)
Where fullVisitorId in (
                              SELECT
                              distinct(fullVisitorId)
                              FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
                              , UNNEST(hits) AS hits
                              , UNNEST(hits.product)
                              Where (v2ProductName = "YouTube Men's Vintage Henley"
                              And productRevenue is not null)
)
and productRevenue is not null
and v2ProductName != "YouTube Men's Vintage Henley"
Group by v2ProductName
Order by quantity DESC,v2ProductName;



-- Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 
-- For example, 100% product view then 40% add_to_cart and 10% purchase.
-- Add_to_cart_rate = number product  add to cart/number product view. 
-- Purchase_rate = number product purchase/number product view. 
-- The output should be calculated in product level.

-- Option1: 
With view_cart as 
      (SELECT 
            format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
            sum(case when eCommerceAction.action_type = '2' then 1 else 0 end) as num_product_view ,
            sum(case when eCommerceAction.action_type = '3' then 1 else 0 end)  as num_addtocart
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
      ,UNNEST(hits) AS hits
            , UNNEST(hits.product)
      where _table_suffix between '0101'and '0331'
      Group by month
      Order by month),

purchase_action as 
(SELECT 
      format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
      sum(case when eCommerceAction.action_type = '6' then 1 else 0 end) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
,UNNEST(hits) AS hits
       , UNNEST(hits.product)
where _table_suffix between '0101'and '0331'
And productRevenue is not null
Group by month
Order by month)

SELECT *, 
      Round (100*num_addtocart/num_product_view,2) as add_to_cart_rate,
      Round (100*num_purchase/num_product_view,2) as purchase_rate
FROM view_cart 
FULL JOIN purchase_action
Using (month)
Order by month;

-- Option 2

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;

