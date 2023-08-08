{{
    config(
        materialized='table',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = 'ID_EDUCATION'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact_tables') }}