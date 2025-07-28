{% snapshot request_snapshot %}

{{
    config(
      target_schema='kestra_unit_test_us',
      unique_key='unique_key',
      strategy='check',
      check_cols=['source', 'status', 'status_change_date']
    )
}}

WITH deduplicated AS (
    SELECT
        unique_key,
        source,
        status,
        status_change_date,
        ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY status_change_date DESC) as rn
    FROM {{ ref('requests') }}
    LIMIT 10
)

SELECT
    unique_key,
    source,
    status,
    status_change_date
FROM deduplicated
WHERE rn = 1

{% endsnapshot %}