-- The Staging layer is responsible for basic clean up and standardizing names.
-- It does NOT do heavy aggregations.

SELECT
    user_id,
    event_type,
    occurred_at,
    -- Extract the Date component for easier grouping downstream
    toDate(occurred_at) AS event_date,
    -- In ClickHouse, we can use JSON functions to safely extract data from the raw payload
    JSONExtractString(payload, 'test') AS is_test_event
FROM telemetry.events_analytical