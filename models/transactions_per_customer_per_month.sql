with dbt_Part2 as (

    select 
        c.customer_id,
        c.full_name,
        DATE_TRUNC(cct.txn_date, MONTH) as txn_month,
        count (txn_id) as monthly_transaction_count
    from {{ref ('customers')}} c
    join {{ref ('customer_products')}} cp on c.customer_id = cp.customer_id
    join {{ref ('credit_card_transactions')}} cct on cp.customer_product_id = cct.customer_product_id
    group by 
        c.customer_id,
        c.full_name,
        DATE_TRUNC(cct.txn_date, MONTH)
    order by 
        c.customer_id,
        txn_month
)

select
    customer_id,
    full_name,
    txn_month,
    monthly_transaction_count,
    sum(monthly_transaction_count) over (partition by customer_id order by txn_month) as running_transaction_count
from dbt_Part2
order by 
    customer_id,
    txn_month

