{{
    config(
        materialized='table',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = 'ID_INCOME'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact_tables') }}