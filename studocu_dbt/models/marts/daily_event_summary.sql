{{ config(
    materialized='table',
    format='parquet',
    write_compression='snappy',
    partitioned_by=['event_date']
) }}

with events as (
    select * from {{ ref('stg_events') }}
),

aggregated as (
    select
        country_code,
        event_type,
        device_type,
        count(*) as event_count,
        count(distinct user_id) as unique_users,
        count(distinct session_id) as unique_sessions,
        count(distinct document_id) as unique_documents,
        sum(case when is_premium_user then 1 else 0 end) as premium_user_events,
        avg(duration_seconds) as avg_duration_seconds,
        event_date
    from events
    group by country_code, event_type, device_type, event_date
)

select * from aggregated