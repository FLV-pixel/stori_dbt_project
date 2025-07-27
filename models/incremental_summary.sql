
-- config block

{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'txn_type', 'month'],
    incremental_strategy='merge',
) }}


with dbt_Part3 as (

    select
        c.customer_id,
        cct.txn_type,
        date_trunc(cct.txn_date, month) as month,
        sum(cct.amount) as monthly_amount
    from {{ ref('credit_card_transactions') }} cct
    join {{ ref('customer_products') }} cp
        on cct.customer_product_id = cp.customer_product_id
    join {{ ref('customers') }} c
        on cp.customer_id = c.customer_id
    {% if is_incremental() %}
        where cct.txn_date >= (select max(month) from {{ this }})
    {% endif %}
    group by 
        c.customer_id, 
        cct.txn_type, 
        date_trunc(cct.txn_date, month)

)

select * from dbt_Part3
