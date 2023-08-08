{{ config(materialized='view') }}

WITH dim_INCOME AS(
    SELECT DISTINCT
        NAME_INCOME_TYPE
    FROM {{ ref('application_record') }}
)

SELECT 
    {{ encode_INCOME('NAME_INCOME_TYPE') }} as ID_INCOME, *
FROM 
    dim_INCOME