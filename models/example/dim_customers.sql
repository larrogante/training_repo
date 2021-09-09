{{ config(materialized='table')}}

select a.id userid
,a.first_name
,a.last_name
,min(b.order_date) first_order_date
,max(b.order_date) most_recent_order_date
,count(b.id) order_cnt
from scg-udp-dw-dev.dbt_training_len.jaffle_shop_customers a
left join scg-udp-dw-dev.dbt_training_len.jaffle_shop_orders b
   on a.id = b.user_id
group by 1,2,3