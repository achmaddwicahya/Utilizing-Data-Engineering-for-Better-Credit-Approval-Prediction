{{ config(materialized='view') }}

WITH dim_EDUCATION AS(
    SELECT DISTINCT
        NAME_EDUCATION_TYPE
    FROM {{ ref('application_record') }}
)

SELECT 
    {{ encode_EDUCATION('NAME_EDUCATION_TYPE') }} as ID_EDUCATION, NAME_EDUCATION_TYPE
FROM 
    dim_EDUCATION