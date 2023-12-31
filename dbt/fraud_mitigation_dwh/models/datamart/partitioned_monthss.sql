{{
    config(
        materialized='table',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact_tables') }}