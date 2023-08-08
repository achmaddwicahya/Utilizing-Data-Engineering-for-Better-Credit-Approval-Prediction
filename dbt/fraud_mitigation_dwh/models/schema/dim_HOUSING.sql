{{ config(materialized='view') }}

WITH dim_HOUSING AS(
    SELECT DISTINCT
        NAME_HOUSING_TYPE
    FROM {{ ref('application_record') }}
)

SELECT 
    {{ encode_HOUSING('NAME_HOUSING_TYPE') }} as ID_HOUSING, *
FROM 
    dim_HOUSING