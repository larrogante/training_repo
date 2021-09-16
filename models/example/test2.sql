
{{ config( materialized = 'view' ) }}
select distinct paymentmethod 
from scg-udp-dw-dev.dbt_training_len.stripe_payments
