{% snapshot request_snapshot %}

{{
    config(
      target_schema='kestra_unit_test_us',
      unique_key='unique_key',
      strategy='check',
      check_cols=['source', 'status', 'status_change_date']
    )
}}

SELECT
    unique_key,
    source,
    status,
    status_change_date
FROM {{ ref('requests') }}
    LIMIT 10

{% endsnapshot %}