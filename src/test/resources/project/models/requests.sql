{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

SELECT
    unique_key,
    source,
    status,
    zipcode_name,
    status_change_date
FROM `bigquery-public-data.austin_311.311_service_requests`
         LEFT JOIN {{ ref('zipcode') }} ON zipcode_id = incident_zip
WHERE city IS NOT NULL
-- this filter will only be applied on an incremental run
    {% if is_incremental() %}
  AND status_change_date > (SELECT MAX(status_change_date) FROM {{ this }})
    {% endif %}


