with Part1 as (

    select
        c.customer_id,
        c.full_name,
        cct.txn_type,
        sum(cct.amount) as total_amount
    from {{ source('stori_test', 'credit_card_transactions') }} cct
    join {{ source('stori_test', 'customer_products') }} cp
        on cct.customer_product_id = cp.customer_product_id
    join {{ source('stori_test', 'customers') }} c
        on cp.customer_id = c.customer_id
    group by c.customer_id, c.full_name, cct.txn_type

)

select * from Part1
