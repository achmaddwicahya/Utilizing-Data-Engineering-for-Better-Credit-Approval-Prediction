{{ config(materialized='table') }}

with fact_tables as (
    SELECT * 
    FROM {{ ref('application_record') }}
),
parameters AS (
  SELECT
    DATE '2023-01-01' start_date,
    DATE '2023-07-31' finish_date 
)

SELECT
    DATE_FROM_UNIX_DATE(CAST(start + (finish - start) * RAND() AS INT64)) AS DATE_APPLY,
    fact_tables.ID,
    ie.ID_EDUCATION,
    ih.ID_HOUSING,
    ii.ID_INCOME,
    ij.ID_JOB,
    im.ID_MARITAL,
    fact_tables.CODE_GENDER,
    fact_tables.FLAG_OWN_CAR,
    fact_tables.FLAG_OWN_REALTY,
    fact_tables.CNT_CHILDREN,
    fact_tables.AMT_INCOME_TOTAL,
    fact_tables.FLAG_MOBIL,
    fact_tables.FLAG_WORK_PHONE,
    fact_tables.FLAG_PHONE,
    fact_tables.FLAG_EMAIL,
    fact_tables.CNT_FAM_MEMBERS,
    fact_tables.YEARS_AGE,
    fact_tables.YEARS_EMPLOYED
FROM
    fact_tables, parameters, UNNEST([STRUCT(UNIX_DATE(start_date) AS start, UNIX_DATE(finish_date) AS finish)])
    INNER JOIN {{ ref('dim_EDUCATION') }} ie ON fact_tables.NAME_EDUCATION_TYPE = ie.NAME_EDUCATION_TYPE 
    INNER JOIN {{ ref('dim_HOUSING') }} ih ON fact_tables.NAME_HOUSING_TYPE = ih.NAME_HOUSING_TYPE
    INNER JOIN {{ ref('dim_INCOME') }} ii ON fact_tables.NAME_INCOME_TYPE = ii.NAME_INCOME_TYPE
    INNER JOIN {{ ref('dim_MARITAL') }} im ON fact_tables.NAME_FAMILY_STATUS = im.NAME_FAMILY_STATUS
    LEFT JOIN {{ ref('dim_JOB') }} ij ON fact_tables.OCCUPATION_TYPE = ij.OCCUPATION_TYPE
    