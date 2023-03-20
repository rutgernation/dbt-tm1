-- saleslog_monthly.sql

{{ config(materialized='table') }}

with saleslog_monthly as (
    select to_char(date_trunc('month', s.ts::timestamp), 'YYYY-MM-DD') as month,
        sum(s.amount * s.totalvalue) as total_sales
    from {{ source('public', 'saleslog') }} s
    group by month
)

select * from saleslog_monthly
