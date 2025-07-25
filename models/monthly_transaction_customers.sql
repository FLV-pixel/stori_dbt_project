with m_t_c as (

    select
        c.customer_id,
        date_trunc(t.txn_date, month) as txn_month,
        count(*) as monthly_txn_count
    from {{ source('stori_test', 'credit_card_transactions') }} t
    join {{ source('stori_test', 'customer_products') }} cp
        on t.customer_product_id = cp.customer_product_id
    join {{ source('stori_test', 'customers') }} c
        on cp.customer_id = c.customer_id
    group by c.customer_id, date_trunc(t.txn_date, month)

), running_total as (

    select
        customer_id,
        txn_month,
        monthly_txn_count,
        sum(monthly_txn_count) over (
            partition by customer_id
            order by txn_month
        ) as running_txn_total
    from m_t_c

)

select * from running_total
