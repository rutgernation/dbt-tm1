-- saleslog_monthly.sql

{{ config(materialized='table') }}

SELECT TO_CHAR(DATE_TRUNC('month', s.ts::timestamp), 'YYYY-MM-DD') AS month,
       SUM(s.amount * s.totalvalue) AS total_sales
FROM {{ source('public', 'saleslog') }} s
GROUP BY month
ORDER BY month ASC;
