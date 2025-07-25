{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'txn_type', 'month']
) }}

with base as (

    select
        c.customer_id,
        t.txn_type,
        date_trunc(t.txn_date, month) as month,
        sum(t.amount) as monthly_amount
    from {{ source('stori_test', 'credit_card_transactions') }} t
    join {{ source('stori_test', 'customer_products') }} cp
        on t.customer_product_id = cp.customer_product_id
    join {{ source('stori_test', 'customers') }} c
        on cp.customer_id = c.customer_id

    {% if is_incremental() %}
        where t.txn_date > (select max(month) from {{ this }})
    {% endif %}

    group by c.customer_id, t.txn_type, date_trunc(t.txn_date, month)

)

select * from base
