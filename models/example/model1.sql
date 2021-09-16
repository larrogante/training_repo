/*
select a.id orderid
,b.first_name
,b.last_name
,a.min(order_date) first_order_date
,a.max(order_date) most_recent_order_date
,a.status order_status
,c.paymentmethod
,c.status payment_status
from scg-udp-dw-dev.dbt_training_len.jaffle_shop_orders a
left join scg-udp-dw-dev.dbt_training_len.jaffle_shop_customers b 
   on b.id = a.user_id
left join scg-udp-dw-dev.dbt_training_len.stripe_payments c 
   on c.orderid = a.id
*/

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



--select * from scg-udp-dw-dev.dbt_training_len.jaffle_shop_orders limit 10


   --select a.* from scg-udp-dw-dev.dbt_training_len.stripe_payments a