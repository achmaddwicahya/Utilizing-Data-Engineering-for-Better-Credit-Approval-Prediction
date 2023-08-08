{{ config(materialized='view') }}

WITH dim_MARITAL AS(
    SELECT DISTINCT
        NAME_FAMILY_STATUS
    FROM {{ ref('application_record') }}
)

SELECT 
    {{ encode_MARITAL('NAME_FAMILY_STATUS') }} as ID_MARITAL, *
FROM 
    dim_MARITAL