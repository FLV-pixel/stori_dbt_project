with dbt_Part1 as (
    select 
        c.customer_id,
        c.full_name,
        cct.txn_type,
        sum(cct.amount) as total_amount
    from {{ ref('customers') }} c
    inner join {{ref('customer_products')}} cp 
    on c.customer_id = cp.customer_id 
    join {{ ref('credit_card_transactions') }} cct 
    on cp.customer_product_id = cct.customer_product_id 

    group by 
        c.customer_id, 
        c.full_name, 
        cct.txn_type

)

select * from dbt_Part1



