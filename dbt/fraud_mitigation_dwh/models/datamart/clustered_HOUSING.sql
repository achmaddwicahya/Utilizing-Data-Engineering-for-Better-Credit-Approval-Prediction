{{
    config(
        materialized='table',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = 'ID_HOUSING'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact_tables') }}