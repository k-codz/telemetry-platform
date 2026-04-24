
  
    
    
    
        
         


        
  

  insert into `telemetry`.`daily_event_summary`
        ("event_date", "event_type", "total_events", "unique_users")-- The Marts layer combines staging models and runs heavy aggregations
-- to provide clean, ready-to-use data for business dashboards.

-- Notice the ref() function! dbt automatically knows it must run stg_events first.
WITH staging_events AS (
    SELECT * FROM `telemetry`.`stg_events`
)

SELECT
    event_date,
    event_type,
    COUNT(user_id) AS total_events,
    -- countDistinct is highly optimized in ClickHouse for finding Unique Users
    countDistinct(user_id) AS unique_users
FROM staging_events
GROUP BY
    event_date,
    event_type
  