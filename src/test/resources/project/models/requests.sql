{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

WITH deduplicated AS (
    SELECT
        unique_key,
        source,
        status,
        status_change_date,
        ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY status_change_date DESC) as rn
    FROM `bigquery-public-data.austin_311.311_service_requests`
    LEFT JOIN {{ ref('zipcode') }} ON zipcode_id = CAST(incident_zip as INTEGER)
    WHERE city IS NOT NULL
    {% if is_incremental() %}
        AND status_change_date > (SELECT MAX(status_change_date) FROM {{ this }})
    {% endif %}
)

SELECT
    unique_key,
    source,
    status,
    status_change_date
FROM deduplicated
WHERE rn = 1