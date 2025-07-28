# Take-Home dbt Exam – Stori Test

## Autor: [Fernando Abel Leal Villavicencio]


---

## Parte 1: Basic Data Transformation
In this first part, we configure the specified tables according to the following parameters:

## Table: `customers`

**Structure:**
- `customer_id` (`int`)
- `full_name` (`string`)
- `email` (`string`)
- `signup_date` (`date`)
- `status` (`string`)

---

## Table: `customer_products`

**Structure:**
- `customer_product_id` (`int`)
- `customer_id` (`int`)
- `product_id` (`int`)
- `opened_date` (`date`)
- `status` (`string`)

---

## Table: `credit_card_transactions`

**Structure:**
- `txn_id` (`int`)
- `customer_product_id` (`int`)
- `txn_date` (`date`)
- `amount` (`float`)
- `txn_type` (`string`) — e.g., `"purchase"` or `"payment"`

To achieve this, certain considerations must be taken into account within the project. First, we can define these tables based on CSV files, from which we can generate a `schema.yml` file to define the table characteristics to be stored in our data warehouse. This process will be carried out using dbt. However, we can also create the tables directly in our data warehouse and define their characteristics manually.
Based on the above, and for the purpose of this test, we will use CSV files located in our seeds folder.

## `seed/schema.yml`
```yaml
version: 2

seeds:
  - name: customers
    config:
      column_types:
        customer_id: INTEGER
        full_name: STRING
        email: STRING
        signup_date: DATE
        status: STRING

  - name: customer_products
    config:
      column_types:
        customer_product_id: INTEGER
        customer_id: INTEGER
        product_id: INTEGER
        opened_date: DATE
        status: STRING

  - name: credit_card_transactions
    config:
      column_types:
        txn_id: INTEGER
        customer_product_id: INTEGER
        txn_date: DATE
        amount: FLOAT64
        txn_type: STRING
```

Within the same file, certain considerations must be taken into account, mainly regarding the date and number formats, as these could cause errors when running the project.

First, the date format: by default, dates are expected to follow the ISO 8601 format (YYYY-MM-DD). If this is not the case, an error will occur.

Another important aspect is defining the number type used in the `credit_card_transactions` table, specifically in the `amount` column. This will also depend on the data warehouse being used. In this case, for `BigQuery`, we cannot define the precision of a number directly in the schema file, so we might instead enforce two decimal places directly in a query within `BigQuery`.


## Parte 1: Tasks

In this first task, we define the characteristics needed to create our new model. Initially, we are told that it should be a model that allows us to determine the total amount of transactions per customer. To achieve this, it must include the following information:

**Content:**
- `customer_id`
- `full_name`
- `txn_type`
- `total_amount`
  
**grouped by:**
- `customer_id`
- `txn_type`
  
To do this, we need to calculate the `total amount`, which can be done through a quick sum query on the amounts contained in our `credit_card_transactions` table. Therefore, our model is defined as follows:

## `models\transactions_summary_by_customer.sql`
```sql
with dbt_Part1 as (   -- We define our CTE in this case from the first task of part 1.
    select 
        c.customer_id,
        MIN(c.full_name) AS full_name, -- We use MIN to avoid adding the full_name column inside the group by. 
        cct.txn_type,
        sum(cct.amount) as total_amount  -- In this section, we define that we want to sum the amounts of the transactions made.
    from {{ ref('customers') }} c
    join {{ref('customer_products')}} cp 
    on c.customer_id = cp.customer_id 
    join {{ ref('credit_card_transactions') }} cct 
    on cp.customer_product_id = cct.customer_product_id 

    group by 
        c.customer_id,  
        cct.txn_type

)

select * from dbt_Part1

```
Within our `schema` file, we implement a dbt `test` that verifies the column `total_amount` never contains null values (`NULL`) in the `transactions_summary_by_customer` model. This way, if there were any null values, we could check that the calculation implemented within our query is indeed correct. We can add additional tests; however, for the purpose of this first task, using a test only on `total_amount` would be sufficient.

## `models\schema.yml`
```yml
# Part 1
models:
  - name: transactions_summary_by_customer
    columns:
      - name: customer_id
      - name: full_name
      - name: txn_type
      - name: total_amount
        tests:
          - not_null    -- We apply a not\_null test to avoid empty amounts.
```
---

## Parte 2: Basic Analysi
For this second part, it is necessary to analyze customer behavior over time. Therefore, we need to define new features for this new model.
In this case, we need the number of transactions that customers make within a given time period. To do this, we require information such as:

**Content:**
- `customer_id`
- `full_name`
- `txn_date`
- `txn_id`
  
Additionally, we are asked to use a window function `OVER` to calculate the number of transactions for each customer up to a given date. To do this, we will divide the query into `two parts`: the first part calculates the number of transactions per month for each customer, and the second part calculates a cumulative count of transactions per customer over time.

## `models\transactions_per_customer_per_month.sql`
```sql

with dbt_Part2 as (        -- The first part calculates the number of transactions per month for each customer

    select 
        c.customer_id,
        min(c.full_name) as full_name,
        DATE_TRUNC(cct.txn_date, MONTH) as txn_month,
        count (txn_id) as monthly_transaction_count
    from {{ref ('customers')}} c
    join {{ref ('customer_products')}} cp on c.customer_id = cp.customer_id
    join {{ref ('credit_card_transactions')}} cct on cp.customer_product_id = cct.customer_product_id
    group by 
        c.customer_id,
        DATE_TRUNC(cct.txn_date, MONTH)
    order by 
        c.customer_id,
        txn_month
)

select        -- The second part calculates a cumulative count of transactions per customer. 
    customer_id,
    full_name,
    txn_month,
    monthly_transaction_count,
    sum(monthly_transaction_count) over (partition by customer_id order by txn_month) as running_transaction_count        --`OVER` indicates that a window function is being used.
from dbt_Part2
order by 
    customer_id,
    txn_month
```
We update the schema file and implement tests to verify that we are performing the correct calculation for each part of the previous query. In this case, we apply two tests: one on `txn_month` to ensure that all rows have a valid month value, and another on `running_transactions_count` to guarantee the quality validation of the model.

## `models\schema.yml`
```yml
# Part 2

  - name: transactions_per_customer_per_month
    columns:
      - name: customer_id
      - name: full_name
      - name: txn_month
        tests:
          - not_null
      - name: running_transactions_count
        tests:
          - not_null
```

---

## Parte 3: Incremental Loading

In this third part of the test, we assume that the `credit_card_transactions` table can receive daily updates. Therefore, we need to define additional configurations in the `schema.yml` file and in the project base file `project.yml`.
To do this, we update our schema file with the necessary configuration and add a validation to ensure that the data we obtain is correct.

## `models\schema.yml`
```yml
# Part 3

  - name: incremental_summary
    columns:
      - name: customer_id
      - name: txn_type
      - name: month
      - name: monthly_amount
        tests:
          - not_null
```
On the other hand, in our `dbt_project.yml` file, we add the following configuration:

## `dbt_project.yml`
```yml
# Model configurations
models:
  Stori_Test:
      +materialized: table
      incremental_summary:
        +materialized: incremental  -- The models in that subfolder will be incremental, meaning they are not recreated each time.

# Seed configurations
seeds:
  Stori_Test:
    +quote_columns: false
```

This means that the models within the `incremental_summary/` folder will be materialized as `incremental`, which means they will only update new or changed rows.

Once we have defined the main features for our model to work correctly, we can create a query based on incremental logic. To do this, at the beginning of our query, we define a configuration block that specifies the model’s behavior during execution.

In this case, we add the property `materialized='incremental'`. This means that after the first run, subsequent executions will only insert or update new or modified data instead of reloading the entire table.

We use `unique_key=['customer_id', 'txn_type', 'month']` so that dbt can identify which rows to update.

Finally, the incremental strategy is set to `merge` to ensure that the model’s behavior is to insert and update, thus avoiding the default strategy used by the data warehouse we’re working with.

## `models\incremental_summary.sql`
### `Configuration`
```sql
-- config block

{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'txn_type', 'month'],
    incremental_strategy='merge',
) }}
```
Within our CTE, we can now create an incremental query based on the `txn_date` column. Additionally, we calculate the total monthly amount per customer and the corresponding transaction type. Finally, we add incremental logic by using the `is_incremental()` macro and including a `WHERE` clause.

## `models\incremental_summary.sql`
```sql
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
    {% if is_incremental() %}  --Filters rows with the most recent txn_date
        where cct.txn_date >= (select max(month) from {{ this }})  --Returns the most recent date already recorded in the table
    {% endif %}
    group by 
        c.customer_id, 
        cct.txn_type, 
        date_trunc(cct.txn_date, month)

)

select * from dbt_Part3

```

## Part 4: dbt Fundamentals

### Summary

In summary, dbt is a command-line tool that allows us to perform robust data processing using SQL queries while providing full integration with various compatible data warehouses. This enables us to guarantee data quality and its processing through tests and documentation, which help detect errors and ensure that data meets the defined criteria for each project. Its main goal is to facilitate the transformation of data already loaded in a data warehouse and then integrate it into the analysis workflow using SQL queries.

For this evaluation, I consider the three key components to be:

- `Models`: This component allows us to create the necessary queries through SQL files to properly process the data according to the defined logic.

- `schema.yml`: This is the main configuration file for our models; however, multiple schema files can be defined depending on the project needs. In it, we define the source and the type of tests.

- `dbt_project.yml`: This is the project configuration file where we define dbt’s behavior. Without this file, the project cannot run.