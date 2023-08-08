{{ config(materialized='view') }}

WITH dim_JOB AS(
    SELECT DISTINCT
        OCCUPATION_TYPE
    FROM {{ ref('application_record') }}
)

SELECT 
    {{ encode_JOB('OCCUPATION_TYPE') }} as ID_JOB, *
FROM 
    dim_JOB