{% snapshot request_snapshot %}

{{
    config(
      target_schema='kestra_unit_test_us',
      unique_key='unique_key',
      strategy='timestamp',
      updated_at='status_change_date',
    )
}}

select * from {{ ref('requests') }}

{% endsnapshot %}
